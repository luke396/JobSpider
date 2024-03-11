# Used for testing the Chrome browser with selenium
# If you not install webdriver,
# this script will install it automatically by webdriver_manager

# Notice that, ipv6 will cause some error, diable it if you have.
import requests

from spider import logger
from spider.utility import Proxy, build_driver

proxy = Proxy(local=False).get()

requests_response = requests.get(
    "https://www.baidu.com", proxies={"http": proxy}, timeout=10
)
logger.info(f"Response: {requests_response.text}")

driver = build_driver(headless=True, proxy=proxy)
driver.get("https://www.baidu.com")
logger.info(f"Response: {driver.page_source}")

driver.quit()
logger.close()
