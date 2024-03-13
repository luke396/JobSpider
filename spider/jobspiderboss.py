"""This is a spider for Boss."""

import json
import time
import urllib.parse
from pathlib import Path

from bs4 import BeautifulSoup
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as EC  # noqa: N812
from selenium.webdriver.support.ui import WebDriverWait

from spider import logger
from utility.constant import (
    MAX_RETRIES,
    WAIT_TIME,
)
from utility.path import BOSS_COOKIES_FILE_PATH, JOBOSS_SQLITE_FILE_PATH
from utility.proxy import Proxy
from utility.selenium_ext import (
    build_driver,
    random_click,
    random_scroll,
    random_sleep,
)
from utility.sql import execute_sql_command

# Boss limit 10 pages for each query
# if add more query keywords, result will be different


class LoginManager:
    """Login manager for Boss."""

    def __init__(self, driver: WebDriver) -> None:
        """Init."""
        self.driver = driver
        self.cookies = None
        self.url = "https://www.zhipin.com/web/user/?ka=header-login"

    def login(self, timeout: int = 60) -> None:
        """Login and get cookies within timeout."""
        logger.info("Start to login")
        if self._cache_login():
            logger.info("Login success using cache cookies")
            return

        self._login_manually(timeout)

    def _login_force(self) -> None:
        """Force login."""
        self._clear_cookies()
        self._login_manually()

    def _login_manually(self, timeout: int = 60) -> None:
        """Login manually."""
        logger.info("Please login manually")
        self.driver.get(self.url)
        self.cookies = self.driver.get_cookies()

        # Wait for login
        start_time = time.time()
        while time.time() - start_time < timeout:
            self.cookies = self.driver.get_cookies()
            if self._valid_cookie():
                with Path(BOSS_COOKIES_FILE_PATH).open("w") as f:
                    json.dump(self.cookies, f)
                logger.info("Login success")
                break
        else:
            logger.error("Login timed out")

    def _cache_login(self) -> bool:
        """Login and get cookies."""
        self._read_cookies()
        if self._valid_cookie():
            self._update_cookies()
            return True
        return False

    def _valid_cookie(self) -> bool:
        """Check if the cookies are valid."""
        if self.cookies is None:
            return False
        cookie_names = {cookie["name"] for cookie in self.cookies}
        required_cookies = {"geek_zp_token", "zp_at"}
        return required_cookies.issubset(cookie_names)

    def _clear_cookies(self) -> None:
        """Clear cookies."""
        self.driver.delete_all_cookies()
        self.cookies = None
        if Path(BOSS_COOKIES_FILE_PATH).exists():
            Path.unlink(BOSS_COOKIES_FILE_PATH)

    def _read_cookies(self) -> None:
        """Read cookies from file."""
        if Path(BOSS_COOKIES_FILE_PATH).exists():
            with Path(BOSS_COOKIES_FILE_PATH).open("r") as f:
                self.cookies = json.load(f)

    def _update_cookies(self) -> None:
        """Update cookies."""
        self.driver.get(self.url)
        for cookie in self.cookies:
            self.driver.add_cookie(cookie)
        logger.info("Update cookies")


class JobSpiderBoss:
    """This is a spider for Boss.

    Crawl one keyword in one city for all pages.
    """

    driver: WebDriver

    def __init__(self, keyword: str, city: str) -> None:
        """Init."""
        self.keyword = keyword
        self.city = city
        self.page = 1
        self.max_page = 10
        self.proxy = Proxy(local=False)

    def start(self) -> None:
        """Crawl by building the page url."""
        while self.page <= self.max_page:
            self.url = self._build_url(self.keyword, self.city)
            self._crwal_single_page_by_url()

            if self.page == 1:
                self._get_max_page()

            self.page += 1

            self.driver.quit()

    def _build_url(self, keyword: str, city: str) -> str:
        """Build the URL for the job search."""
        base_url = "https://www.zhipin.com/web/geek/job"
        query_params = urllib.parse.urlencode(
            {"query": keyword, "city": city, "page": self.page}
        )
        return f"{base_url}?{query_params}"

    def _build_driver(self) -> WebDriver:
        self.driver = build_driver(headless=False, proxy=self.proxy.get())

    def _crwal_single_page_by_url(self) -> str:
        """Get the HTML from the URL."""
        job_list = None
        for _ in range(MAX_RETRIES):
            try:
                self._get()

                job_list = (
                    WebDriverWait(self.driver, WAIT_TIME)
                    .until(
                        EC.presence_of_element_located((By.CLASS_NAME, "job-list-box"))
                    )
                    .get_attribute("innerHTML")
                )

                if job_list is None:
                    logger.error("job_list is None, maybe proxy is blocked, retrying")
                    break

                random_click(self.driver, 10.0)
                random_scroll(self.driver)
                break

            except TimeoutException:
                logger.error("TimeoutException of getting job list, retrying")
                self.driver.quit()
                continue

        self._parse_job_list(job_list)

    def _get(self) -> None:
        self._build_driver()
        logger.info(f"Crawling {self.url}")
        random_sleep()
        random_sleep()
        random_sleep()

        self.driver.get(self.url)
        random_sleep()

        random_click(self.driver, 5.0)

    def _get_max_page(self) -> None:
        max_page = int(
            self.driver.find_elements(By.CLASS_NAME, "options-pages")[0].text[-1]
        )
        if max_page != 0:  # last charactor is 10, but str select 0 out
            self.max_page = int(max_page)
            logger.info(f"Update max page to {self.max_page}")

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
    JobSpiderBoss(keyword, area_code).start()
