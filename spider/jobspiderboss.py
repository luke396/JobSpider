"""This is a spider for Boss."""

import asyncio
import random
import traceback
from collections.abc import AsyncGenerator, Coroutine, Generator
from contextlib import asynccontextmanager, suppress
from urllib.parse import urlencode

from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    Response,
    async_playwright,
)
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from selenium.webdriver.remote.webdriver import WebDriver

from spider import logger
from utility.constant import (
    MAX_RETRIES,
    MAX_WAIT_TIME,
    MIN_WAIT_TIME,
)
from utility.path import JOBOSS_SQLITE_FILE_PATH
from utility.proxy import Proxy
from utility.sql import (
    create_joboss_url_pool_table,
    execute_sql_command,
)


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
        async_play: Playwright,
        *,
        local_proxy: bool,
    ) -> None:
        """Init."""
        self.keyword = keyword
        self.city = city
        self.proxies: Proxy = Proxy(local=local_proxy)

        self.cur_page_num: int = 1
        self.max_page_num: int = 10

        self.async_play = async_play

    def _chunked_tasks(
        self, tasks: list[Coroutine], chunk_size: int
    ) -> Generator[list[Coroutine], None, None]:
        """Yield successive chunks from tasks."""
        for i in range(0, len(tasks), chunk_size):
            yield tasks[i : i + chunk_size]

    @asynccontextmanager
    async def _managed_browser(self) -> AsyncGenerator[Browser, None]:
        browser: Browser = await self._build_browser()
        try:
            yield browser
        finally:
            await browser.close()

    @asynccontextmanager
    async def _managed_page(
        self, url: str, context: BrowserContext
    ) -> AsyncGenerator[Page | None, None]:
        """Create a new page and close it after use.

        If the page is banned or some other error, return None.
        """
        page, banned = await self._get_cur_page(url, context)
        try:
            if banned:
                yield None
            else:
                yield page
        finally:
            await page.close()

    async def update_max_page(self) -> None:
        """Update the max page."""
        if self._check_in_page_table():
            logger.info(f"Keyword {self.keyword} of {self.city} already in page table")
            return

        url = build_single_url(self.keyword, self.city, self.cur_page_num)

        async with self._managed_browser() as browser:
            for _ in range(MAX_RETRIES):
                context = await self._update_context(browser)
                async with self._managed_page(url, context) as page:
                    if not page:
                        logger.warning("Page not found, retrying")
                        continue

                    page_content = await page.content()

                max_page = await self._parse_max_page(page_content)
                if max_page:
                    self._insert_maxpage_to_db(max_page)
                    logger.info(f"Insert max page {max_page} to page table")
                    break

            else:
                logger.error(f"Failed to update max page after {MAX_RETRIES} retries")

    async def start(self) -> None:
        """Crawl by building the page url."""
        await self.update_max_page()

        tasks = [
            self._crwal_single_page_by_url(page)
            for page in range(self.cur_page_num, self.max_page_num + 1)
        ]

        for chunk in self._chunked_tasks(tasks, 2):
            await asyncio.gather(*chunk)

    async def _build_browser(self) -> Browser:
        return await self.async_play.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--enable-automation=true",
                "--disable-blink-features=AutomationControlled",
            ],
        )

    async def _update_context(self, browser: Browser) -> BrowserContext:
        """Update the context with a new proxy."""
        return await browser.new_context(
            locale="zh-CN",
            user_agent=UserAgent().random,
            proxy={"server": self.proxies.get()},
            extra_http_headers={"Accept-Encoding": "gzip"},
        )

    async def _get_cur_page(
        self, url: str, context: BrowserContext
    ) -> tuple[Page, bool]:
        await asyncio.sleep(random.uniform(MAX_WAIT_TIME, MIN_WAIT_TIME))

        page: Page = await context.new_page()
        await page.evaluate("navigator.webdriver = false")

        logger.info(f"Visiting {url}")

        try:
            async with page.expect_response(
                "https://www.zhipin.com/wapi/zpgeek/search/joblist.json*"
            ) as response_info:
                await page.goto(url)

                # wait and check if ip banned
                with suppress(PlaywrightTimeoutError):
                    await page.wait_for_load_state("networkidle", timeout=10000)
                if await self._check_ip_banned(page):
                    return (page, True)

                job_result = page.locator("div.search-job-result ul.job-list-box")
                await job_result.wait_for(timeout=2000)

                response: Response = await response_info.value
                logger.info(str(response))
                return (page, False)

        except PlaywrightTimeoutError:
            logger.warning(f"Timeout when visiting {url}")

        except PlaywrightError as e:
            logger.error(
                f"Error of type {type(e).__name__} when visiting {url}. Message: {e!s}"
            )
            logger.error(f"Stack trace: {traceback.format_exc()}", exc_info=True)

        return (page, True)

    async def _check_ip_banned(self, page: Page) -> bool:
        try:
            content = await page.content()
        except PlaywrightError:
            logger.warning("Failed to check ip banned.")
            return False

        banned_phrases = [
            "您暂时无法继续访问~",
            "当前 IP 地址可能存在异常访问行为，完成验证后即可正常使用.",  # noqa: RUF001
        ]
        if any(phrase in content for phrase in banned_phrases):
            logger.warning("IP banned")
            return True
        return False

    async def _query_job_list(self, page: Page) -> str:
        job_list_ele = await page.query_selector(
            "div.search-job-result ul.job-list-box"
        )
        if job_list_ele:
            return await job_list_ele.inner_html()
        return ""

    async def _parse_max_page(self, page_context: str) -> int | None:
        soup: BeautifulSoup = BeautifulSoup(page_context, "html.parser")
        max_page_ele = soup.find("div", class_="options-pages")

        if max_page_ele is None:
            logger.error("Failed to find max page element")
            return None

        max_page_text: str = max_page_ele.text
        max_page: int = int(max_page_text[-1])

        if max_page == 0:  # last character is 10, but str select 0 out
            max_page = 10

        return max_page

    async def _get_page_joblist(
        self, url: str, context: BrowserContext, cur_page_num: int
    ) -> str:
        page = await self._get_cur_page(url, context)
        if not page:
            logger.error(f"Failed to get page {cur_page_num}")
            return ""

        job_list = await self._query_job_list(page)
        if job_list:
            await self._parse_max_page(page, cur_page_num)
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

    def _check_in_page_table(self) -> bool:
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
        if result:
            return True
        return False


def build_single_url(keyword: str, city: str, page_num: int) -> str:
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


async def _build_url_pool(keyword: str, city: str, max_page: int) -> None:
    """Build the URL pool of single city for the job search."""
    insert_sql = """
    INSERT INTO `joboss_url_pool` (
        `url`,
        `keyword`,
        `area_code`,
        `visited`,
        `cur_page`,
        `max_page`
    ) VALUES (:url, :keyword, :area_code, :visited, :cur_page, :max_page);
    """
    urls = [
        {
            "url": build_single_url(keyword, city, cur_page),
            "keyword": keyword,
            "area_code": city,
            "visited": 0,
            "cur_page": cur_page,
            "max_page": max_page,
        }
        for cur_page in range(1, max_page + 1)
    ]
    execute_sql_command(insert_sql, JOBOSS_SQLITE_FILE_PATH, urls)


def build_url_pool() -> None:
    """Build the total URL pool for the job search."""
    create_joboss_url_pool_table()
    results = execute_sql_command(
        """SELECT `keyword`, `area_code`, `max_page` FROM `joboss_max_page`;""",
        JOBOSS_SQLITE_FILE_PATH,
    )
    if not results:
        logger.error("No data in joboss_max_page table")
        return

    for keyword, area_code, max_page in results:
        asyncio.run(_build_url_pool(keyword, area_code, max_page))


async def update_page(keyword: str, area_code: str) -> None:
    """Start the spider."""
    async with async_playwright() as playwright:
        spider = JobSpiderBoss(keyword, area_code, playwright, local_proxy=False)
        await spider.update_max_page()
