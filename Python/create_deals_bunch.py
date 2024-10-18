import hubspot
from pprint import pprint
from hubspot.crm.deals import SimplePublicObjectInputForCreate, ApiException
from creds2 import hs_test

import random
import schedule
import time

def create_deal():
    client = hubspot.Client.create(access_token= hs_test)

    # Variable para almacenar el número
    nr = random.randint(10000, 99999)

    properties = {
        "amount": "1500.00",
        "dealname": f"Custom{nr}",
        "pipeline": "default",
        "closedate": "2019-12-07T16:50:06.678Z",
        "dealstage": "presentationscheduled"
    }
    simple_public_object_input_for_create = SimplePublicObjectInputForCreate(properties=properties)
    try:
        api_response = client.crm.deals.basic_api.create(simple_public_object_input_for_create=simple_public_object_input_for_create)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling basic_api->create: %s\n" % e)


# Programa la ejecución de la función cada 2 segundos
schedule.every(2).seconds.do(create_deal)

# Mantén el programa en ejecución
while True:
    schedule.run_pending()
    time.sleep(1)  # Espera 1 segundo entre cada iteración para evitar uso excesivo de recursos