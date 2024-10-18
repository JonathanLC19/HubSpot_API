import selenium.webdriver as webdriver
import time
from bs4 import BeautifulSoup
from tabulate import tabulate
def scrape_website(website):
    print("Connecting to Scraping Browser...")

    chrome_driver_path = "./chromedriver"
    options = webdriver.ChromeOptions()

    # Ejecutar en modo headless (sin mostrar la ventana del navegador)
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")  # Para entornos sin UI, puede ser necesario
    options.add_argument("--disable-dev-shm-usage")  # Opcional, para evitar problemas en algunos entornos
    
    driver = webdriver.Chrome(options=options)

    try:
        driver.get(website)
        # print("Waiting for you to solve the captcha manually...")
        # input("Resuelve el captcha y presiona Enter para continuar...")  # Pausa para resolver captcha

        print("Scraping page content...")
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        content = soup.find('div', {'class': 'section post-body', 'id': 'post-body', 'itemprop': 'articleBody'})
        title = soup.find('h1', {'itemprop': 'name headline'})
        if title:
            title = title.find('span', {'id': 'hs_cos_wrapper_name'}).text

        time.sleep(10)  # Espera adicional si es necesario
        
        return {
            "title": title,
            "content": content.get_text()
        }
    
    finally:
        driver.quit()

result = scrape_website("https://knowledge.hubspot.com/properties/create-and-edit-properties")
print(f"Title: {result['title']}")
# print(f"Content: {result['content']}")

import sqlite3

def insert_into_hs_kb(title, category, object, content):
    conn = sqlite3.connect('example.db')
    cursor = conn.cursor()

    cursor.execute('''
        INSERT INTO hs_kb (title, category, object, text)
        VALUES (?, ?, ?, ?)
    ''', (title, category, object, content))

    conn.commit()
    conn.close()

# Example usage:
title = result['title']
category = 'properties'
object = 'all'
content = result['content']
# insert_into_hs_kb(title, category, object, content)

print("\n HubSpot Knowledge Base")
print("-----------------------")
conn = sqlite3.connect('example.db')
cursor = conn.cursor()
cursor.execute('SELECT * FROM hs_kb')
rows = cursor.fetchall()
for row in rows:
    print(f"{row[0]:<4} {row[1]:<40} {row[2]:<20} {row[3]:<15} {row[4]:<15}")

conn.commit()
conn.close()
