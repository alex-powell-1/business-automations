from bs4 import BeautifulSoup
from selenium import webdriver
import time
from setup import creds


def get_gift_cards():
    gift_cards = []
    driver = webdriver.Chrome()
    driver.get(creds.gift_card_url)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    table = soup.find('table')
    for row in table.find_all('tr'):
        cells = row.find_all('td')
        if len(cells) == 4:
            gift_cards.append(
                {'name': cells[0].text, 'amount': cells[1].text, 'expiration': cells[2].text, 'code': cells[3].text}
            )
    return gift_cards


if __name__ == '__main__':
    chrome_options = webdriver.ChromeOptions()
    # chrome_options.add_argument('--headless=new')

    driver = webdriver.Chrome(options=chrome_options)
    url = 'https://admin.shopify.com/store/settlemyre-test/gift_cards?selectedView=all'
    driver.get(url)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    # print(soup)

    time.sleep(3)

    email = driver.find_element(value='account_email')
    email.send_keys('lukebbarrier06@outlook.com\n')

    time.sleep(3)
    password = driver.find_element(value='account_password')
    password.send_keys(f'{creds.shopify_ac}\n')

    time.sleep(3)

    driver.get(url)

    time.sleep(10)
