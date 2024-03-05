"""This file is used to store the configuration of the spider."""
from webdriver_manager.chrome import ChromeDriverManager

MAX_RETRIES = 3
MIN_SLEEP = 1
MAX_SLEEP = 3
CHROMESERVICEPATH = ChromeDriverManager().install()

PROXY_GROUP = [  # set your proxy group
    "http://localhost:30001",
    "http://localhost:30002",
    "http://localhost:30003",
]

# 51job
FIREWALL_MESSAGE = "很抱歉，由于您访问的URL有可能对网站造成安全威胁，您的访问被阻断"
