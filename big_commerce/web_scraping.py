from setup import creds
from selenium import webdriver
from bs4 import BeautifulSoup
import time
import pandas as pd

driver = webdriver.Chrome()
for k, v in creds.competitor_bank.items():
    print(f"Beginning Scraping for {k}")
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
