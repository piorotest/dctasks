import os
import sys
import yaml
from yaml.loader import SafeLoader

from dct import common
from dct import database


common.BASE_URL=os.getenv('URL')
common.API_KEY=os.getenv('API_KEY')
task_name=os.getenv('TASK_NAME')
pipeline_build=os.getenv('PIPENAME')


print(f"Connecting to DCT: {common.BASE_URL}")
if pipeline_build:
    print(f"adding pipeline suffix to bookmarks: {pipeline_build}")

with open('dctasks.yml') as f:
   task_list = yaml.load(f, Loader=SafeLoader)

task = [ x for x in task_list if x["name"] == task_name ]

if len(task) != 1:
    print("Task not found or not unique")
    sys.exit(-1)


if task[0]["action"] == "create_bookmark":
    if pipeline_build:
        bookmark_name = task[0]["bookmark_name"] + f"_{pipeline_build}"
    else:
        bookmark_name = task[0]["bookmark_name"]
    if database.create_bookmark(task[0]["source"], bookmark_name):
        print("Error with creating bookmark")
        sys.exit(-1)
    else:
        print("bookmark created")
        sys.exit(0)

if task[0]["action"] == "delete_bookmark":
    if pipeline_build:
        bookmark_name = task[0]["bookmark_name"] + f"_{pipeline_build}"
    else:
        bookmark_name = task[0]["bookmark_name"]
    if database.delete_bookmark(bookmark_name):
        print("Error with deleting bookmark")
        sys.exit(-1)
    else:
        print("bookmark deleted")
        sys.exit(0)


if task[0]["action"] == "list_bookmark":
    if database.list_bookmark(task[0]["database"]):
        print("Error with listing bookmark")
        sys.exit(-1)
    else:
        sys.exit(0)


if task[0]["action"] == "refresh_from_bookmark":
    if pipeline_build:
        bookmark_name = task[0]["bookmark_name"] + f"_{pipeline_build}"
    else:
        bookmark_name = task[0]["bookmark_name"]
    if database.refresh_vdb(task[0]["database"], bookmark_name = bookmark_name):
        print("Error with refreshing database from bookmark")
        sys.exit(-1)   
    else:
        print("Database refreshed")
        sys.exit(0)

if task[0]["action"] == "refresh_from_latest_snapshot":
    if database.refresh_vdb(task[0]["database"], snapshot_id = 'LATEST'):
        print("Error with refreshing database from latest snapshot")
        sys.exit(-1)   
    else:
        print("Database refreshed")
        sys.exit(0)


if task[0]["action"] == "create_vdb_from_bookmark":
    if pipeline_build:
        bookmark_name = task[0]["bookmark_name"] + f"_{pipeline_build}"
    else:
        bookmark_name = task[0]["bookmark_name"]
    if database.create_database(task[0]["database"], task[0]["server"], task[0]["oracle_home"], bookmark_name = bookmark_name):
        print("Error with refreshing database from bookmark")
        sys.exit(-1)   
    else:
        print("Database refreshed")
        sys.exit(0)

#print(database.find_virtual_database('pipedb'))

# if database.delete_bookmark('dupa3') == 0:
#     print("OK")
# else:
#     print("NOT OK")








