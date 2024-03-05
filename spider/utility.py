"""Utility functions for the spider."""

import random
from pathlib import Path

from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService

from spider import logger
from spider.config import (
    CHROME_SERVICE_PATH,
    PROXY_GROUP,
)

# ref: https://stackoverflow.com/a/73519818/16493978


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
    """Init webdriver.

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
    options.add_argument("--proxy-server=" + random.choice(PROXY_GROUP))

    web = webdriver.Chrome(service=service, options=options)
    web.execute_script(
        'Object.defineProperty(navigator, "webdriver", {get: () => false,});',
    )

    logger.info("Building webdriver done")

    return web
