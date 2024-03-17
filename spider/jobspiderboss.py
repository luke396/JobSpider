"""This is a spider for Boss."""

import asyncio
import random
import traceback
from collections.abc import AsyncGenerator, Coroutine, Generator
from contextlib import asynccontextmanager, suppress
from urllib.parse import urlencode

from bs4 import BeautifulSoup, NavigableString, ResultSet, Tag
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

from spider import logger
from utility.constant import (
    MAX_RETRIES,
    MAX_WAIT_TIME,
    MIN_WAIT_TIME,
)
from utility.path import JOBOSS_SQLITE_FILE_PATH
from utility.proxy import Proxy
from utility.sql import (
    execute_sql_command,
)


# Boss limit 10 pages for each query
# if add more query keywords, result will be different
class JobSpiderBoss:
    """This is a spider for Boss.

    If url is not None, use the url to crawl.
    If keyword and city is not None, use the keyword and city to update the max page,
    not to crawl.
    """

    def __init__(
        self,
        async_play: Playwright,
        keyword: str | None = None,
        city: str | None = None,
    ) -> None:
        """Init."""
        self.keyword = keyword or ""
        self.city = city or ""

        self.proxies: Proxy = Proxy(local=False)
        self.async_play = async_play

    def _chunked_tasks(
        self, tasks: list[Coroutine], chunk_size: int
    ) -> Generator[list[Coroutine], None, None]:
        """Yield successive chunks from tasks.

        Used to limit the number of concurrent tasks.
        """
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

    async def _build_browser(self) -> Browser:
        return await self.async_play.chromium.launch(
            headless=False,
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

    async def update_max_page(self) -> None:
        """Update the max page."""
        if not self.keyword or not self.city:
            logger.error("Keyword or city is not set")
            return

        if self._check_in_page_table():
            logger.info(f"Keyword {self.keyword} of {self.city} already in page table")
            return

        url = build_single_url(self.keyword, self.city, 1)

        async with self._managed_browser() as browser:
            for _ in range(MAX_RETRIES):
                context = await self._update_context(browser)
                async with self._managed_page(url, context) as page:
                    if not page:
                        logger.warning("Page not found, retrying")
                        continue

                    page_content = await page.content()
                    break

            else:
                logger.error(f"Failed to update max page after {MAX_RETRIES} retries")

        max_page = await self._parse_max_page(page_content)
        if max_page:
            self._insert_max_page_to_db(max_page)
            logger.info(f"Insert max page {max_page} to page table")

    async def crawl(self, urls: list[str]) -> None:
        """Crawl by building the page url."""
        tasks = [self._crwal_single_page_by_url(url) for url in urls]

        for chunk in self._chunked_tasks(tasks, min(2, len(tasks))):
            await asyncio.gather(*chunk)

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

    async def _crwal_single_page_by_url(self, url: str) -> None:
        """Get the HTML from the URL."""
        async with self._managed_browser() as browser:
            for _ in range(MAX_RETRIES):
                context = await self._update_context(browser)
                async with self._managed_page(url, context) as page:
                    if not page:
                        logger.warning("Page not found, retrying")
                        continue

                    page_content = await page.content()
                    break
            else:
                logger.error(f"Failed to crawl {url} after {MAX_RETRIES} retries")
                return

        jobs = await self._parse_job_list(page_content)
        self._insert_job_to_db(jobs)

    async def _parse_job_list(self, page_content: str) -> list[tuple[str, ...]]:
        """Parse the HTML and get the JSON data."""
        soup = BeautifulSoup(page_content, "html.parser")
        job_card: ResultSet[Tag] = soup.find_all("li", class_="job-card-wrapper")
        return [tuple(self._parse_single_job(job).values()) for job in job_card]

    def _parse_single_job(self, job: Tag) -> dict:
        def _get_text(
            element: Tag | NavigableString | None, class_name: str, name: str = "span"
        ) -> str:
            if isinstance(element, NavigableString):
                return ""
            ele = element.find(name, class_=class_name) if element else None
            return ele.text if ele else ""

        def _join_text(element: Tag | NavigableString | None) -> str:
            if isinstance(element, NavigableString):
                return ""
            return ",".join(li.text for li in element.find_all("li")) if element else ""

        def _get_info(job: Tag, class_name: str) -> Tag | None:
            info = job.find("div", class_=class_name)
            return info if isinstance(info, Tag) else None

        _job_info = _get_info(job, "job-info clearfix")
        _company_info = _get_info(job, "company-info")
        _card_bottom = _get_info(job, "job-card-footer clearfix")

        return {
            "job_name": _get_text(job, "job-name"),
            "job_area": _get_text(job, "job-area"),
            "job_salary": _get_text(job, "salary"),
            "edu_exp": (
                _join_text(_job_info.find("ul", class_="tag-list")) if _job_info else ""
            ),
            "company_name": _get_text(_company_info, "company-name", name="h3")
            if _company_info
            else "",
            "company_tag": _join_text(_company_info) if _company_info else "",
            "skill_tags": _join_text(_card_bottom.find("ul")) if _card_bottom else "",
            "job_other_tags": (
                _get_text(_card_bottom, "info-desc", name="div").replace("，", ",")  # noqa: RUF001
                if _card_bottom
                else ""
            ),
        }

    def _insert_job_to_db(self, jobs: list) -> None:
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

    def _insert_max_page_to_db(self, max_page: int) -> None:
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


def create_joboss_url_pool_table() -> None:
    """Create the table in the database."""
    sql_table = """
    CREATE TABLE IF NOT EXISTS joboss_url_pool (
        url TEXT,
        keyword TEXT,
        area_code TEXT,
        visited INTEGER DEFAULT 0,
        cur_page INTEGER,
        max_page INTEGER,
        PRIMARY KEY (url)
    );
    """
    execute_sql_command(sql_table, JOBOSS_SQLITE_FILE_PATH)


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


def _random_select_url() -> str:
    """Random select a url from the pool."""
    select_sql = """
    SELECT `url` FROM `joboss_url_pool` WHERE `visited` = 0 ORDER BY RANDOM() LIMIT 1;
    """
    result = execute_sql_command(select_sql, JOBOSS_SQLITE_FILE_PATH)
    if result:
        return result[0][0]
    return ""


def _update_url_visited(url: str) -> None:
    """Update the url visited status."""
    update_sql = """
    UPDATE `joboss_url_pool` SET `visited` = 1 WHERE `url` = :url;
    """
    execute_sql_command(update_sql, JOBOSS_SQLITE_FILE_PATH, {"url": url})


async def update_page(keyword: str, area_code: str) -> None:
    """Start the spider."""
    async with async_playwright() as playwright:
        spider = JobSpiderBoss(
            async_play=playwright,
            keyword=keyword,
            city=area_code,
        )
        await spider.update_max_page()


async def crawl_single() -> None:
    """Crawl a single page from the random selected url."""
    url = _random_select_url()
    async with async_playwright() as playwright:
        await JobSpiderBoss(playwright).crawl([url])


if __name__ == "__main__":
    asyncio.run(crawl_single())
