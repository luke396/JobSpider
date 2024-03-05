"""This file is used to store the configuration of the spider."""

from pathlib import Path

from webdriver_manager.chrome import ChromeDriverManager

from spider.utility import create_output_dir

MAX_RETRIES = 3
MIN_SLEEP = 1
MAX_SLEEP = 3
CHROME_SERVICE_PATH = ChromeDriverManager().install()
PROXY_GROUP = [  # set your proxy group
    "http://localhost:30001",
    "http://localhost:30002",
    "http://localhost:30003",
]
FIREWALL_MESSAGE = "很抱歉，由于您访问的URL有可能对网站造成安全威胁，您的访问被阻断"  # noqa: RUF001

AREA_DB_NAME = "51area.db"
AREA_SQLITE_FILE_PATH = Path(create_output_dir(tag="area")) / AREA_DB_NAME

JOB_DB_NAME = "51job.db"
JOB_SQLITE_FILE_PATH = Path(create_output_dir(tag="job")) / JOB_DB_NAME

SLIDER_XPATH = '//div[@class="nc_bg"]'
WAIT_TIME = 10
MIN_CLICKS = 1
MAX_CLICKS = 3
WIDTH_FACTOR = 3
HEIGHT_FACTOR = 3
MIN_PAUSE = 0.000001
MAX_PAUSE = 0.00005
STEPS = 30
MOVE_DISTANCE = 20
MOVE_VARIANCE = 0.01

# main
AREA_DB_PATH = Path(__file__).resolve().parent / "output" / "area" / "51area.db"
MAX_PAGE_NUM = 20
STRAT_AREA_CODE = 1
END_AREA_CODE = None
KEYWORD = "数据挖掘"
