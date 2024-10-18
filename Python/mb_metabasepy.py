from metabasepy import Client, MetabaseTableParser
import metabase
from pprint import pprint

cli = Client(username=metabase.mb_user, password=metabase.mb_pass, base_url=metabase.mb_url)

cli.authenticate()

query_response = cli.cards.query(card_id="1876")

data_table = MetabaseTableParser.get_table(metabase_response=query_response)
table = data_table.__dict__
cols = data_table.__dict__['cols']

col_list = []

for k in cols:
    name = k.get('name')
    col_list.append(name)

pprint(table)