import time
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver

from setup import creds


def scrape_competitor_prices(log_file):
    """Will login to competitor site, scape HTML, parse to dataframe, and save to .CSV"""
    print(f"Web Scraping: Starting at {datetime.now():%H:%M:%S}", file=log_file)

    driver = webdriver.Chrome()

    for k, v in creds.competitor_bank.items():
        print(f"Beginning Scraping for {k}", file=log_file)
        url = v["site"]
        driver.get(url)

        user = driver.find_element(by="name", value=v['user_input'])
        user.send_keys(v['username'])

        password = driver.find_element(by="name", value=v['pw_input'])
        password.send_keys(v['password'])

        submit_button = driver.find_element(by="name", value=v['submit'])

        time.sleep(1)
        submit_button.click()
        time.sleep(1)

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        table = soup.find('table')

        df = pd.read_html(table.prettify())[0]
        df = df.drop(df.index[0])
        df = df.drop(df.index[0])
        df.to_csv(v['log_location'], header=["Name", "Available", "Size", "Price"], index=False)

    driver.quit()

    print(f"Web Scraping: Completed at {datetime.now():%H:%M:%S}", file=log_file)
    print("-----------------------", file=log_file)
