"""This file is used to store the configuration of the spider."""

import random
import ssl
from pathlib import Path
from typing import Any

import requests
import urllib3
from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

from spider import logger


def create_output_dir(tag: str) -> str:
    """Create output directory if not exists."""
    root = Path(__file__).resolve().parent.parent
    directory = root / f"output/{tag}"

    if not directory.exists():
        directory.mkdir(parents=True)
        logger.info(f"Directory {directory} created.")
    else:
        logger.info(f"Directory {directory} already exists.")
    return str(directory)


def build_driver() -> webdriver:
    """Init webdriver, don't forget to close it.

    During the building process,
    it is necessary to set up an anti crawler detection strategy by Option.

        .add_argument('--no-sandbox')
        -> Disable sandbox mode

        .add_argument('headless')
        -> Set headless page, run silently

        .add_argument('--disable-dev-shm-usage')
        -> Disable shared memory

        .add_argument("--window-size=1920,1080")
        -> In headless status, browse without a window size,
            so if the size of the window is not specified,
        sliding verification may fail

        .add_experimental_option('excludeSwitches',['enable-automation','enable-logging'])
        -> Disable auto control and log feature of the browser

        .add_argument('--disable-blink-features=AutomationControlled')
        -> Disable auto control extension of the browser

        .add_argument(("useAutomationExtension", False))
        -> Disable auto control extension of the browser

        .add_argument(f'user-agent={user_agent}')
        -> Add random UA

    Additionally, if use the visible window execution,
    you need to add the following operations

        .add_argument('--inprivate')
        -> Start by Private Browsing

        .add_argument("--start-maximized")
        -> Maximize the window

    Finally, inject script to change navigator = false to avoid detection.
    """
    user_agent = UserAgent().random
    service = ChromeService(CHROME_SERVICE_PATH)

    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--headless")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_experimental_option(
        "excludeSwitches",
        ["enable-automation", "enable-logging"],
    )
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("useAutomationExtension", value=False)
    options.add_argument(f"user-agent={user_agent}")

    if PROXY_GROUP:  # local not use proxy
        options.add_argument("--proxy-server=" + random.choice(PROXY_GROUP))

    web = webdriver.Chrome(service=service, options=options)
    web.execute_script(
        'Object.defineProperty(navigator, "webdriver", {get: () => false,});',
    )

    logger.info("Building webdriver done")

    return web


class CustomHttpAdapter(requests.adapters.HTTPAdapter):
    """Transport adapter" that allows us to use custom ssl_context."""

    # ref: https://stackoverflow.com/a/73519818/16493978

    def __init__(self, ssl_context: Any = None, **kwargs: str | Any) -> None:  # noqa: ANN401
        """Init the ssl_context param."""
        self.ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(
        self, connections: int, maxsize: int, *, block: bool = False
    ) -> None:
        """Create a urllib3.PoolManager for each proxy."""
        self.poolmanager = urllib3.poolmanager.PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_context=self.ssl_context,
        )


def get_legacy_session() -> requests.Session:
    """Get legacy session."""
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
    session = requests.session()
    session.mount("https://", CustomHttpAdapter(ctx))
    return session


MAX_RETRIES = 3
MIN_SLEEP = 1
MAX_SLEEP = 3
CHROME_SERVICE_PATH = ChromeDriverManager().install()

# if in wsl/windows - code is 0, should use `get_legacy_session()`
# else use `requests.get()` - code is 1
PLAT_CODE = 0

if PLAT_CODE == 0:
    PROXY_GROUP = None
elif PLAT_CODE == 1:
    PROXY_GROUP = [  # set your proxy group
        "http://localhost:30001",
        "http://localhost:30002",
        "http://localhost:30003",
    ]

FIREWALL_MESSAGE = "很抱歉，由于您访问的URL有可能对网站造成安全威胁，您的访问被阻断"  # noqa: RUF001

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

# to avoid circular import
AREA_SQLITE_FILE_PATH = Path(create_output_dir(tag="area")) / "51area.db"
JOB_SQLITE_FILE_PATH = Path(create_output_dir(tag="job")) / "51job.db"

# main
MAX_PAGE_NUM = 20
STRAT_AREA_CODE = 1
END_AREA_CODE = None
KEYWORD = "数据挖掘"
