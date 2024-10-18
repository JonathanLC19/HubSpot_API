from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import time

import credentials as creds

# URL de inicio de sesión y credenciales
login_url = 'https://explorimmobox.explorimmo.com/#/login'
username = creds.lf_user
password = creds.lf_pass
page_url = 'https://explorimmobox.explorimmo.com/#/contact-webs'

# Inicializar el navegador web controlado por Selenium
driver = webdriver.Chrome()  # Ajusta esto según el navegador que estés utilizando y su ubicación

try:
    # Iniciar sesión
    driver.get(login_url)

    # Esperar a que el campo de nombre de usuario sea visible
    username_input = WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located((By.ID, "username"))
    )
    username_input.send_keys(username)

    # Esperar a que el campo de contraseña sea visible
    password_input = WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located((By.ID, "password"))
    )
    password_input.send_keys(password)

    # Hacer clic en el botón de inicio de sesión
    login_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'Login')]"))
    )
    login_button.click()

    # Esperar a que la página se cargue completamente y se complete el inicio de sesión
    time.sleep(5)  # Ajusta esto según sea necesario para esperar que la página se cargue completamente

    # Obtener el HTML de la página después de que se haya ejecutado JavaScript
    driver.get(page_url)
    time.sleep(5)  # Espera adicional para asegurarse de que todo el contenido se haya cargado (ajusta según sea necesario)
    html = driver.page_source

    # Crear el objeto BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')

    # Encontrar la sección con el id "content"
    content_section = soup.find('section', id='content')

    # Verificar si se encontró la sección
    if content_section:
        # Imprimir el contenido dentro de la sección
        content = content_section.prettify()  # Obtener el contenido formateado
        print("Contenido dentro de la sección:")
        print(content)
    else:
        print("No se encontró la sección con el id 'content'")

except TimeoutException:
    print("Error: tiempo de espera excedido al intentar encontrar los elementos en la página.")

finally:
    # Cerrar el navegador
    driver.quit()