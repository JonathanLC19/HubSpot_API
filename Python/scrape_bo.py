import requests
from bs4 import BeautifulSoup
import re

import webdriver_manager

import credentials as creds

bl = "\33[1;36m" #blue text
gr = "\33[0;37m" #grey text
wh = "\33[1;37m" #white text

#region SELENIUM
# ------------------------------------------- SELENIUM -------------------------------------------





#region REQUESTS
# ------------------------------------------- REQUESTS -------------------------------------------

# def page_data(url):
#     """Gives the given url page info"""

#     #output dict
#     d = {}

#     #request headers
#     headers = {
#         'user-agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
#     }

#     #make request
#     print(f'{bl}Executing request: {wh}{url}{gr}')
#     req = requests.get(url, headers=headers, timeout=10)
#     print(f'{bl}Response code...: {wh}{req.status_code} {req.reason}{gr}')

#     #if error
#     if req.status_code != 200:
#         return {"ERROR": f"{req.reason}", "status_code" : f"{req.status_code}"}
    
#     #url
#     d['url'] = req.url

#     soup = BeautifulSoup(req.text, 'html.parser')
#     print(req.text)


# page_data("https://developers.hubspot.com/beta-docs/reference/api/crm/objects/deals?uuid=35121985-9c38-4b9c-b90d-51a6cbedb335")

















# # URL de inicio de sesión y credenciales
# login_url = 'https://prod.backoffice.ukio.com/'
# username = creds.bo_user
# password = creds.bo_pass
# page_url = 'https://prod.backoffice.ukio.com/apartments'

# # Crear sesión
# session = requests.Session()

# # Datos de inicio de sesión
# login_data = {
#     'Email': username,
#     'Password': password
# }

# # Iniciar sesión
# login_response = session.post(login_url, data=login_data)

# # Verificar si el inicio de sesión fue exitoso
# if login_response.status_code == 200:
#     # Si el inicio de sesión fue exitoso, ahora puedes acceder a páginas protegidas
#     # Solicitar la página deseada después del inicio de sesión
#     page_response = session.get('https://prod.backoffice.ukio.com/apartments')

#     # Analizar la página con BeautifulSoup
#     soup = BeautifulSoup(page_response.content, 'html.parser')

#     # Aquí puedes realizar el scraping que necesites con BeautifulSoup
#     # Por ejemplo, encontrar elementos por etiqueta, clase, etc.
# else:
#     print("Fallo en el inicio de sesión")