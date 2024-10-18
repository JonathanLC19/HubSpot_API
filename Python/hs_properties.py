import hubspot
from pprint import pprint
from hubspot.crm.properties import PropertyCreate, ApiException, PropertyUpdate
from hubspot.crm.objects.calls import ApiException

from credentials import hs_prod_key, hs_sb_key
import pandas as pd
import json

"""--------------------------------------- READ PROPS---------------------------------------------"""

# def readProps(api_key, object_type):
#     client = hubspot.Client.create(access_token=api_key)

#     try:
#         api_response = client.crm.properties.core_api.get_all(object_type=object_type, archived=False)
#         # pprint(api_response)
#     except ApiException as e:
#         print("Exception when calling core_api->get_all: %s\n" % e)

#     props = api_response.results

#     return props
# with open('openai_app/pages/sb_reservations_props.txt', 'w') as f:
#     for prop in readProps(hs_sb_key, "bookings"):
#         f.write(f"{prop}\n")
"""--------------------------------------- READ ONE PROP---------------------------------------------"""
def readSingleProp(key, object, property):
    obj_type = object
    client = hubspot.Client.create(access_token=key)

    try:
        get_prop = client.crm.properties.core_api.get_by_name(object_type=obj_type, property_name=property, archived=False)
        # pprint(get_prop)
    except ApiException as e:
        print("Exception when calling core_api->get_by_name: %s\n" % e)
    
    return get_prop

prop = (readSingleProp(hs_prod_key, "tickets", "invoice_sent_to_ap"))
# pprint(prop)
# for option in prop.options:
#     print(option.label)


"""--------------------------------------- CREATE PROP ---------------------------------------------"""

# client = hubspot.Client.create(access_token=hs_prod_key)

# property_create = PropertyCreate(
#     hidden="false", 
#     label=prop.label, 
#     type=prop.type, 
#     form_field=prop.form_field, 
#     group_name=prop.group_name, 
#     name= prop.name, 
#     options=prop.options,
#     has_unique_value=prop.has_unique_value, 
#     field_type=prop.field_type
# )
# try:
#     api_response = client.crm.properties.core_api.create(object_type="tickets", property_create=property_create)
#     pprint(api_response)
# except ApiException as e:
#     print("Exception when calling core_api->create: %s\n" % e)

"""--------------------------------------- UPDATE PROP ---------------------------------------------"""

def updateProp(key, object, property, prop):
    obj_type = object
    client = hubspot.Client.create(access_token=key)
    property_update = PropertyUpdate(
        hidden="false", 
        label=prop.label, 
        type=prop.type, 
        form_field=prop.form_field, 
        group_name=prop.group_name,
        name= prop.name, 
        options=prop.options,
        has_unique_value=prop.has_unique_value, 
        field_type=prop.field_type
    )

    try:
        update_prop = client.crm.properties.core_api.update(object_type=obj_type, property_name=property, property_update=property_update)
        # pprint(update_prop)
    except ApiException as e:
        print("Exception when calling core_api->update: %s\n" % e)

    return update_prop

# client = hubspot.Client.create(access_token="YOUR_ACCESS_TOKEN")

# property_update = PropertyUpdate(group_name="contactinformation", hidden=False, options=[{"label":"Option A","value":"A","hidden":false,"description":"Choice number one","displayOrder":1},{"label":"Option B","value":"B","hidden":false,"description":"Choice number two","displayOrder":2}], display_order=2, description="string", calculation_formula="string", label="My Contact Property", type="enumeration", field_type="select", form_field=True)
# try:
#     api_response = client.crm.properties.core_api.update(object_type="objectType", property_name="propertyName", property_update=property_update)
#     pprint(api_response)
# except ApiException as e:
#     print("Exception when calling core_api->update: %s\n" % e)