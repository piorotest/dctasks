import json
from dct import common

def find_virtual_database(database_name:str, fqdn:str=None) -> str:
    if fqdn:
        filter_exp = filter_exp + " AND fqdn EQ f'{fqdn}' "
    else:
        filter_exp = f"name EQ '{database_name}'"
    filter_exp_obj = {
        "filter_expression": filter_exp
    }

    res = common.DCT_POST('vdbs/search', payload=filter_exp_obj)
    
    if res:
        if (res["response_metadata"]["total"] != 1):
            print("Database doesn't exist or is not unique")
            return 1
        else:
            return res["items"][0]["id"]


def create_bookmark(database_names: str, bookmark_name: str):

    vdb_obj = database_names.split(',')
    bookmark_obj = {
        "name": bookmark_name,
        "vdb_ids": vdb_obj
    }

    res = common.DCT_POST('bookmarks', payload=bookmark_obj)
    if res:
        common.wait_for_job(res["job"]["id"])      
    else:
        return 1

    return 0


def list_bookmark(database_names: str, fqdn:str=None):

    filter_array = []
    for vdb_name in database_names.split(','):
        filter_array.append(f"vdb_ids CONTAINS '{find_virtual_database(vdb_name)}'")


    filter_exp_obj = {
        "filter_expression": f""" { " OR ".join(filter_array) } """
    }

    res = common.DCT_POST('bookmarks/search?sort=creation_date', payload=filter_exp_obj)
    
    if res:
        for bookmark in res["items"]:
            print(bookmark["name"])



def delete_bookmark(bookmark_name: str):
    bookmark_obj = f'bookmarks/{bookmark_name}'
    res = common.DCT_DELETE(bookmark_obj)
    if res:
        return 0
    else:
        return 1


def refresh_vdb(database_name:str, fqdn:str=None, **kwargs):
    # todo: implement other
    bookmark_name = kwargs.get('bookmark_name')
    snapshot_id = kwargs.get('snapshot_id')

    dbid = find_virtual_database(database_name, fqdn)
    print(f"DATABASE ID {dbid}")

    if bookmark_name:
        vdb_refresh = {
            "bookmark_id": bookmark_name
        }
        url = f"vdbs/{dbid}/refresh_from_bookmark"

    if snapshot_id:
        if snapshot_id == 'LATEST':
            vdb_refresh = {}
        else:
            vdb_refresh = {
                "snapshot_id": snapshot_id
            }
        url = f"vdbs/{dbid}/refresh_by_snapshot"


    if dbid:        
        res = common.DCT_POST(url, payload=vdb_refresh)
        if res:
            common.wait_for_job(res["job"]["id"])      
        else:
            return 1
    else:
        return 1

    return 0

def create_database(database_name:str, fqdn:str, repository:str, **kwargs):
    # todo: implement other
    bookmark_name = kwargs.get('bookmark_name')

    create_obj = {
        "target_group_id": "Oracle 19c Virtual Databases",
        "name": database_name,
        "database_name": database_name,
        "environment_id": fqdn,
        "repository_id": repository,
        "template_id": "vdbtemplate",
        "online_log_size": 50,
        "online_log_groups": 3,
        "bookmark_id": bookmark_name
    }


    url = f"vdbs/provision_from_bookmark"
    res = common.DCT_POST(url, payload=create_obj)
    if res:
        common.wait_for_job(res["job"]["id"])      
    else:
        return 1


    return 0