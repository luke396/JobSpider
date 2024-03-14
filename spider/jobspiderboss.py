"""This is a spider for Boss."""

import random
import urllib.parse

from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from playwright.sync_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    sync_playwright,
)
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
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
        self, keyword: str, city: str, playwright: Playwright, *, local_proxy: bool
    ) -> None:
        """Init."""
        self.keyword = keyword
        self.city = city
        self.proxies: Proxy = Proxy(local=local_proxy)

        self.cur_page_num: int = 1
        self.max_page_num: int = 10

        self.playwright = playwright
        self.browser: Browser = None
        self.context: BrowserContext = None
        self.page: Page = None

        self._build_browser()

    def start(self) -> None:
        """Crawl by building the page url."""
        while self.cur_page_num <= self.max_page_num:
            self.url = self._build_url(self.keyword, self.city)
            self._crwal_single_page_by_url()

            self.cur_page_num += 1

    def _build_url(self, keyword: str, city: str) -> str:
        """Build the URL for the job search."""
        base_url = "https://www.zhipin.com/web/geek/job"
        query_params = urllib.parse.urlencode(
            {"query": keyword, "city": city, "page": self.cur_page_num}
        )

        fake_param = {
            "industry": "",
            "jobType": "",
            "experience": "",
            "salary": "",
            "degree": "",
            "scale": "",
            "stage": "",
        }
        for _ in range(random.randint(1, 3)):
            if fake_param:
                random_key = random.choice(list(fake_param.keys()))
                del fake_param[random_key]

        query_params += "&" + urllib.parse.urlencode(fake_param)
        return f"{base_url}?{query_params}"

    def _build_browser(self) -> None:
        browser_type = self.playwright.chromium
        launch_args = {
            "headless": False,
            "args": [
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--enable-automation=true",
                "--disable-blink-features=AutomationControlled",
                "--lang=zh-CN,zh;q=0.9",
            ],
        }
        curr_proxy = self.proxies.get()
        if curr_proxy != "":
            launch_args["proxy"] = {"server": curr_proxy}

        self.browser = browser_type.launch(**launch_args)

    def _update_context(self) -> None:
        if self.context:
            self.context.close()
        self.context = self.browser.new_context(
            locale="zh-CN",
            user_agent=UserAgent(os=["windows", "macos"]).random,
        )

    def _get_page_joblist(self) -> str:
        job_list = None
        self.page = self.context.new_page()
        self.page.evaluate("navigator.webdriver = undefined")
        try:
            self.page.goto(self.url)
            logger.info(f"Visiting {self.url}")

            self.page.wait_for_timeout(5000)
            self.page.wait_for_selector("div.search-job-result")
            job_list = self.page.query_selector(
                "div.search-job-result ul.job-list-box"
            ).inner_html()

            if self.cur_page_num == 1:
                self._get_max_page()

        except AttributeError:
            logger.error(
                f"Failed to get the job list from {self.url}, maybe get page too quick"
            )
        except PlaywrightTimeoutError:
            logger.error(f"Timeout of {self.url}, retry")
        else:
            return job_list
        finally:
            self.page.close()

    def _crwal_single_page_by_url(self) -> str:
        """Get the HTML from the URL."""
        for _ in range(MAX_RETRIES):
            self._update_context()
            job_list = self._get_page_joblist()
            if job_list is not None:
                break

        self._parse_job_list(job_list)

    def _get_max_page(self) -> None:
        max_page_element = self.page.query_selector(".options-pages")
        max_page_text = max_page_element.inner_text()
        max_page = int(max_page_text[-1])
        if max_page != 0:  # last character is 10, but str select 0 out
            self.max_page_num = int(max_page)
            logger.info(f"Update max page to {self.max_page_num}")

    def _parse_job_list(self, job_list: str) -> None:
        """Parse the HTML and get the JSON data."""
        soup = BeautifulSoup(job_list, "html.parser")
        job_card = soup.find_all("li", class_="job-card-wrapper")
        jobs = [tuple(self._parse_job(job).values()) for job in job_card]
        self._insert_to_db(jobs)

    def _parse_job(self, job: BeautifulSoup) -> dict:
        job_name = job.find("span", class_="job-name").text
        job_area = job.find("span", class_="job-area").text
        job_salary = job.find("span", class_="salary").text
        edu_exp = ",".join(
            [
                li.text
                for li in job.find("div", class_="job-info clearfix").find_all("li")
            ]
        )

        _company_info = job.find("div", class_="company-info")
        company_name = _company_info.find("h3", class_="company-name").text
        company_tag = ",".join(
            [
                li.text
                for li in _company_info.find("ul", class_="company-tag-list").find_all(
                    "li"
                )
            ]
        )

        _cardbottom = job.find("div", class_="job-card-footer clearfix")
        skill_tags = ",".join(
            [li.text for li in _cardbottom.find("ul", class_="tag-list").find_all("li")]
        )
        job_other_tags = ",".join(
            _cardbottom.find("div", class_="info-desc").text.split(
                "，"  # noqa: RUF001
            )
        )
        return {
            "job_name": job_name,
            "job_area": job_area,
            "job_salary": job_salary,
            "edu_exp": edu_exp,
            "company_name": company_name,
            "company_tag": company_tag,
            "skill_tags": skill_tags,
            "job_other_tags": job_other_tags,
        }

    def _insert_to_db(self, jobs: dict) -> None:
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
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """

        execute_sql_command(sql, JOBOSS_SQLITE_FILE_PATH, jobs)


def _create_table() -> None:
    """Create the table in the database."""
    sql_table = """
    CREATE TABLE IF NOT EXISTS `joboss` (
        `job_name` TEXT NULL,
        `area` TEXT NULL,
        `salary` TEXT NULL,
        `edu_exp` TEXT NULL,
        `company_name` TEXT NULL,
        `company_tag` TEXT NULL,
        `skill_tags` TEXT NULL,
        `job_other_tags` TEXT NULL,
        PRIMARY KEY (`job_name`, `company_name`, `area`, `salary`, `skill_tags`)
    );
    """

    execute_sql_command(sql_table, JOBOSS_SQLITE_FILE_PATH)


def start(keyword: str, area_code: str) -> None:
    """Start the spider."""
    _create_table()
    with sync_playwright() as playwright:
        JobSpiderBoss(keyword, area_code, playwright, local_proxy=True).start()
