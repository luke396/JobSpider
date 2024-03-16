"""This is a spider for Boss."""

import asyncio
import contextlib
import random
import traceback
from collections.abc import Coroutine
from urllib.parse import urlencode

from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    async_playwright,
)
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from selenium.webdriver.remote.webdriver import WebDriver

from spider import logger
from utility.constant import (
    MAX_RETRIES,
)
from utility.path import JOBOSS_SQLITE_FILE_PATH
from utility.proxy import Proxy
from utility.sql import execute_sql_command


# Boss limit 10 pages for each query
# if add more query keywords, result will be different
class JobSpiderBoss:
    """This is a spider for Boss.

    Crawl one keyword in one city for all pages.
    """

    driver: WebDriver

    def __init__(
        self,
        keyword: str,
        city: str,
        async_playwright: Playwright,
        *,
        local_proxy: bool,
    ) -> None:
        """Init."""
        self.keyword = keyword
        self.city = city
        self.proxies: Proxy = Proxy(local=local_proxy)

        self.cur_page_num: int = 1
        self.max_page_num: int = 10

        self.async_playwright = async_playwright
        self.browser: Browser = None

    def _chunked_tasks(self, tasks: Coroutine, chunk_size: int) -> Coroutine:
        """Yield successive chunks from tasks."""
        for i in range(0, len(tasks), chunk_size):
            yield tasks[i : i + chunk_size]

    async def update_max_page(self) -> None:
        """Update the max page."""
        if self._check_in_page_table():
            logger.info(f"Keyword {self.keyword} of {self.city} already in page table")
            return

        max_page_num = None
        await self._build_browser()

        for _ in range(MAX_RETRIES):
            context = await self._update_context()
            page = await self._get_cur_page(
                self._build_url(self.keyword, self.city, self.cur_page_num), context
            )
            if page:
                break
            logger.warning("Update maxpage failed, retrying")
        else:
            logger.error(f"Failed to update max page after {MAX_RETRIES} retries")
            await self.browser.close()
            return

        try:
            max_page_num = await self._update_max_page(page)
        except ValueError:
            # failed to get max page, pass
            logger.warning(f"Failed to get max page from {self.keyword} of {self.city}")
        finally:
            await page.close()
            await self.browser.close()

        if max_page_num:
            self._insert_maxpage_to_db(max_page_num)
            logger.info(f"Insert max page {max_page_num} to page table")

    async def start(self) -> None:
        """Crawl by building the page url."""
        await self.update_max_page()

        tasks = [
            self._crwal_single_page_by_url(page)
            for page in range(self.cur_page_num, self.max_page_num + 1)
        ]

        for chunk in self._chunked_tasks(tasks, 2):
            await asyncio.gather(*chunk)

    def _build_url(self, keyword: str, city: str, page_num: int) -> str:
        """Build the URL for the job search."""
        base_url = "https://www.zhipin.com/web/geek/job"
        query_params = urlencode({"query": keyword, "city": city, "page": page_num})

        fake_param = {
            "industry": "",
            "jobType": "",
            "experience": "",
            "salary": "",
            "degree": "",
            "scale": "",
            "stage": "",
        }

        keys_to_remove = random.sample(list(fake_param), random.randint(2, 5))
        for key in keys_to_remove:
            del fake_param[key]

        query_params += "&" + urlencode(fake_param)
        return f"{base_url}?{query_params}"

    async def _build_browser(self) -> None:
        browser_type = self.async_playwright.chromium
        launch_args = {
            "headless": True,
            "args": [
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--enable-automation=true",
                "--disable-blink-features=AutomationControlled",
            ],
        }

        self.browser = await browser_type.launch(**launch_args)

    async def _update_context(self) -> BrowserContext:
        """Update the context with a new proxy."""
        return await self.browser.new_context(
            locale="zh-CN",
            user_agent=UserAgent().random,
            proxy={"server": self.proxies.get()},
            extra_http_headers={"Accept-Encoding": "gzip"},
        )

    async def _get_cur_page(self, url: str, context: BrowserContext) -> Page:
        await asyncio.sleep(random.uniform(1, 3))
        page = await context.new_page()
        await page.evaluate("navigator.webdriver = false")

        logger.info(f"Visiting {url}")

        try:
            async with page.expect_response(
                "https://www.zhipin.com/wapi/zpgeek/search/joblist.json*"
            ) as response_info:
                await page.goto(url)

                # wait and check if ip banned
                with contextlib.suppress(PlaywrightTimeoutError):
                    await page.wait_for_load_state("networkidle", timeout=5000)
                if await self._check_ip_banned(page):
                    await page.close()
                    return None

                job_result = page.locator("div.search-job-result ul.job-list-box")
                await job_result.wait_for(timeout=2000)

                response = await response_info.value
                logger.info(response)

        except PlaywrightTimeoutError:
            logger.warning(f"Timeout when visiting {url}")
            logger.warning(f"Stack trace: {traceback.format_exc()}", exc_info=True)

        except PlaywrightError as e:
            logger.warning(
                f"Error of type {type(e).__name__} when visiting {url}. Message: {e!s}"
            )
            logger.warning(f"Stack trace: {traceback.format_exc()}", exc_info=True)

        return page

    async def _check_ip_banned(self, page: Page) -> bool:
        content = await page.content()
        banned_phrases = [
            "您暂时无法继续访问~",
            "当前 IP 地址可能存在异常访问行为，完成验证后即可正常使用.",  # noqa: RUF001
        ]
        if any(phrase in content for phrase in banned_phrases):
            logger.error("IP banned")
            return True
        return False

    async def _query_job_list(self, page: Page) -> str:
        job_list_ele = await page.query_selector(
            "div.search-job-result ul.job-list-box"
        )
        if job_list_ele:
            return await job_list_ele.inner_html()
        return None

    async def _update_max_page(self, page: Page) -> str:
        max_page_element = await page.query_selector(".options-pages")
        if max_page_element is None:
            raise ValueError
        max_page_text = await max_page_element.inner_text()
        max_page = int(max_page_text[-1])

        if max_page == 0:  # last character is 10, but str select 0 out
            return 10

        return max_page

    async def _get_page_joblist(
        self, url: str, context: BrowserContext, cur_page_num: int
    ) -> str:
        page = await self._get_cur_page(url, context)
        if not page:
            logger.error(f"Failed to get page {cur_page_num}")
            return None

        job_list = await self._query_job_list(page)
        if job_list:
            await self._update_max_page(page, cur_page_num)
        else:
            logger.error(f"Failed to get job list from page {cur_page_num}")

        await page.close()
        return job_list

    async def _crwal_single_page_by_url(self, cur_page_num: str) -> str:
        """Get the HTML from the URL."""
        url = self._build_url(self.keyword, self.city, cur_page_num)
        job_list = None

        for _ in range(MAX_RETRIES):
            context = await self._update_context()
            job_list = await self._get_page_joblist(url, context, cur_page_num)
            if job_list:
                break
            logger.warning(f"Retry {cur_page_num} page {url}")

        if job_list:
            await self._parse_job_list(job_list)
            logger.info(f"Finish parsing page {cur_page_num}")
        else:
            logger.warning(
                f"Failed to parse page {cur_page_num} after {MAX_RETRIES} retries"
            )

    async def _parse_job_list(self, job_list: str) -> None:
        """Parse the HTML and get the JSON data."""
        soup = BeautifulSoup(job_list, "html.parser")
        job_card = soup.find_all("li", class_="job-card-wrapper")
        jobs = [tuple(self._parse_job(job).values()) for job in job_card]
        self._insert_job_to_db(jobs)

    def _parse_job(self, job: BeautifulSoup) -> dict:
        def _get_text(element: BeautifulSoup, class_name: str) -> str:
            return element.find("span", class_=class_name).text

        def _join_text(element: BeautifulSoup) -> str:
            return ",".join(li.text for li in element.find_all("li"))

        job_info = job.find("div", class_="job-info clearfix")
        company_info = job.find("div", class_="company-info")
        card_bottom = job.find("div", class_="job-card-footer clearfix")

        return {
            "job_name": _get_text(job, "job-name"),
            "job_area": _get_text(job, "job-area"),
            "job_salary": _get_text(job, "salary"),
            "edu_exp": _join_text(
                job_info.find("ul", class_="tag-list"),
            ),
            "company_name": company_info.find("h3", class_="company-name").text,
            "company_tag": _join_text(company_info),
            "skill_tags": _join_text(card_bottom.find("ul")),
            "job_other_tags": ",".join(
                card_bottom.find("div", class_="info-desc").text.split("，")  # noqa: RUF001
            ),
        }

    def _insert_job_to_db(self, jobs: dict) -> None:
        """Insert the data into the database."""
        sql = """
        INSERT INTO `joboss` (
            `job_name`,
            `area`,
            `salary`,
            `edu_exp`,
            `company_name`,
            `company_tag`,
            `skill_tags`,
            `job_other_tags`
        ) VALUES (:job_name, :area, :salary, :edu_exp, :company_name, :company_tag, :skill_tags, :job_other_tags);
        """  # noqa: E501

        execute_sql_command(sql, JOBOSS_SQLITE_FILE_PATH, jobs)

    def _insert_maxpage_to_db(self, max_page: int) -> None:
        """Insert the max page into the database."""
        sql = """
        INSERT INTO `joboss_max_page` (
            `keyword`,
            `area_code`,
            `max_page`
        ) VALUES (:keyword, :area_code, :max_page);
        """

        execute_sql_command(
            sql,
            JOBOSS_SQLITE_FILE_PATH,
            {"keyword": self.keyword, "area_code": self.city, "max_page": max_page},
        )

    def _check_in_page_table(self) -> None:
        """Check if the keyword and area_code is in the page table."""
        sql = """
        SELECT `keyword`, `area_code` FROM `joboss_max_page`
        WHERE `keyword` = :keyword AND `area_code` = :area_code;
        """

        result = execute_sql_command(
            sql,
            JOBOSS_SQLITE_FILE_PATH,
            {"keyword": self.keyword, "area_code": self.city},
        )

        return len(result) != 0


def _create_job_table() -> None:
    """Create the table in the database."""
    sql_table = """
    CREATE TABLE IF NOT EXISTS joboss (
        job_name TEXT,
        area TEXT,
        salary TEXT,
        edu_exp TEXT,
        company_name TEXT,
        company_tag TEXT,
        skill_tags TEXT,
        job_other_tags TEXT,
        PRIMARY KEY (job_name, company_name, area, salary, skill_tags)
    );
    """

    execute_sql_command(sql_table, JOBOSS_SQLITE_FILE_PATH)


def create_max_page_table() -> None:
    """Create the table in the database."""
    sql_table = """
    CREATE TABLE IF NOT EXISTS joboss_max_page (
        keyword TEXT,
        area_code TEXT,
        max_page INTEGER,
        PRIMARY KEY (keyword, area_code)
    );
    """

    execute_sql_command(sql_table, JOBOSS_SQLITE_FILE_PATH)


async def update_page(keyword: str, area_code: str) -> None:
    """Start the spider."""
    async with async_playwright() as playwright:
        spider = JobSpiderBoss(keyword, area_code, playwright, local_proxy=False)
        await spider.update_max_page()


if __name__ == "__main__":
    keywrod = random.choice(
        ["机器学习", "数据挖掘", "人工智能", "深度学习", "计算机视觉", "数据"]
    )
    area_code = random.choice(
        ["101010100", "101020100", "101070200", "101280100", "101060100"]
    )
    asyncio.run(update_page(keywrod, area_code))
