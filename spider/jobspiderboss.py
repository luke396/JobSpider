"""This is a spider for Boss."""

import random
import urllib.parse

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
from utility.path import JOBOSS_SQLITE_FILE_PATH
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
        self.city_code = str(random.choice(["101010100", "101020100", "101280100"]))

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

        # Random select one or two parameters to drop, then add to the query
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

    def _build_driver(self) -> WebDriver:
        self.driver = build_driver(headless=False, proxy=self.proxy.get())

    def _crwal_single_page_by_url(self) -> str:
        """Get the HTML from the URL."""
        job_list = None

        for _ in range(MAX_RETRIES):
            self._build_driver()
            job_list = self._get_joblist()
            if job_list is not None:
                break

        self._parse_job_list(job_list)

    def _get_joblist(self) -> str:
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
        return job_list

    def _get(self) -> None:
        logger.info(f"Crawling {self.url}")
        self.driver.get(self.url)
        self.driver.add_cookie({"name": "lastCity", "value": self.city_code})
        for _ in range(5):
            random_sleep()

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
