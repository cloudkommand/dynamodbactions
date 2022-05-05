import boto3
import botocore
# import jsonschema
import json
import traceback

from extutil import remove_none_attributes, account_context, ExtensionHandler, ext, handle_common_errors
from dynamodb import upsert_rec_robust

eh = ExtensionHandler()

def lambda_handler(event, context):
    try:
        print(f"event = {event}")
        region = account_context(context)['region']
        eh.refresh()
        
        #May want to do something with prev_state, like not overwrite stuff that has already been written
        prev_state = event.get("prev_state")
        cdef = event.get("component_def")
        table_name = cdef.get("table_name")
        items = cdef.get("items")
        if not items:
            eh.add_log("No Items to Write", {"table_name": table_name, "items": items})
            return eh.finish()

        if not isinstance(items, list) and not isinstance(items[0], dict):
            eh.perm_error("items are invalid")

        pass_back_data = event.get("pass_back_data", {})
        if pass_back_data:
            eh.declare_pass_back_data(pass_back_data)
        elif event.get("op") == "upsert":
            eh.add_op("get_table_params")
            eh.add_op("write_recs")

        get_table_params(table_name, region)
        write_recs(table_name, items)
            
        return eh.finish()

    except Exception as e:
        msg = traceback.format_exc()
        print(msg)
        eh.declare_return(200, 0, error_code=str(e))
        return eh.finish()

@ext(handler=eh, op="write_recs")
def write_recs(table_name, items):
    pkey = eh.state['pkey']
    skey = eh.state['skey']

    for i, item in enumerate(items):
        try:
            upsert_rec_robust(table_name, item, pkey_name=pkey, skey_name=skey)
        except botocore.exceptions.ClientError as e:
            handle_common_errors(e, eh, "Error Writing Record", int(100*(i+1)/(len(items) + 1)))
            # print(str(e))
            # eh.add_log(f"Error writing record with partition key value {item.get(pkey)}", {"item": item, "error": e}, is_error=True)
            # eh.perm_error("Write Fail", progress = )
    
    if not eh.error:
        eh.add_log(f"Wrote {len(items)} Records", {"last_item": item})

@ext(handler=eh, op="get_table_params")
def get_table_params(table_name, region):

    dynamodb = boto3.client("dynamodb")

    try:
        description = dynamodb.describe_table(TableName=table_name).get("Table")
        key_schema = description['KeySchema']
        print(f"key_schema = {key_schema}")
        pkey = list(map(lambda x: x['AttributeName'], filter(lambda x: x['KeyType'] == "HASH", key_schema)))[0]
        skey_list = list(map(lambda x: x['AttributeName'], filter(lambda x: x['KeyType'] == "RANGE", key_schema)))
        print(f"pkey = {pkey}")
        print(f"skey_list = {skey_list}")
        eh.add_log("Got Table", description)
        eh.add_state(remove_none_attributes({
            "pkey": pkey,
            "skey": skey_list[0] if skey_list else "#skey:"
        }))
        eh.add_links({"Table": gen_table_link(table_name, region)})
    
    except botocore.exceptions.ClientError as e:
        handle_common_errors(e, eh, "Get Table Failed", 3, ["ResourceNotFoundException"])

def gen_table_link(table_name, region):
    return f"https://console.aws.amazon.com/dynamodb/home?region={region}#tables:selected={table_name};tab=overview"
