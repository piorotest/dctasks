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
user_bookmark_name=os.getenv('BOOKMARK_NAME')
user_database_name=os.getenv('DATABASE_NAME')

print(f"Connecting to DCT: {common.BASE_URL}")
if pipeline_build:
    print(f"adding pipeline suffix to bookmarks: {pipeline_build}")

with open('/code/dctasks.yml') as f:
   task_list = yaml.load(f, Loader=SafeLoader)

task = [ x for x in task_list if x["name"] == task_name ]

if len(task) != 1:
    print("Task not found or not unique")
    sys.exit(-1)


if task[0]["action"] == "create_bookmark":
    if pipeline_build:
        bookmark_name = task[0]["bookmark_name"] + f"_{pipeline_build}"
        database_name = task[0]["database"] + f"_{pipeline_build}"
    else:
        bookmark_name = task[0]["bookmark_name"]
        database_name = task[0]["database"]

    database_name = database_name.replace(".","")

    if database.create_bookmark(database_name, bookmark_name):
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
    if user_bookmark_name:
        bookmark_name = user_bookmark_name
    else:
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
    if user_bookmark_name:
        bookmark_name = user_bookmark_name
    else:
        if pipeline_build:
            bookmark_name = task[0]["bookmark_name"] + f"_{pipeline_build}"
        else:
            bookmark_name = task[0]["bookmark_name"]

    if user_database_name:
        database_name = user_database_name
    else:
        if pipeline_build:
            database_name = task[0]["database"] + f"_{pipeline_build}"
        else:
            database_name = task[0]["database"]

    database_name = database_name.replace(".","")

    print(database_name)

    if database.create_database(database_name, task[0]["server"], task[0]["repository"], bookmark_name = bookmark_name, dbtype = task[0]["dbtype"]):
        print("Error with creating database from bookmark")
        sys.exit(-1)   
    else:
        print("Database refreshed")
        sys.exit(0)


if task[0]["action"] == "create_vdb_from_snapshot":

    if user_database_name:
        database_name = user_database_name
    else:
        if pipeline_build:
            database_name = task[0]["database"] + f"_{pipeline_build}"
        else:
            database_name = task[0]["database"]

    database_name = database_name.replace(".","")

    print(database_name)

    if database.create_database(database_name, task[0]["server"], task[0]["repository"], snapshot_id = task[0]["snapshot_id"], dbtype = task[0]["dbtype"],
                                source = task[0]["source"]):
        print("Error with creating database from snapshot")
        sys.exit(-1)   
    else:
        print("Database refreshed")
        sys.exit(0)


if task[0]["action"] == "drop_vdb":
    if user_database_name:
        database_name = user_database_name
    else:
        if pipeline_build:
            database_name = task[0]["database"] + f"_{pipeline_build}"
        else:
            database_name = task[0]["database"]

    
    database_name = database_name.replace(".","")

    if database.delete_database(database_name, task[0]["server"]):
        print("Error with dropping database")
        sys.exit(-1)   
    else:
        print("Database dropped")
        sys.exit(0)


if task[0]["action"] == "disable_vdb":
    if user_database_name:
        database_name = user_database_name
    else:
        if pipeline_build:
            database_name = task[0]["database"] + f"_{pipeline_build}"
        else:
            database_name = task[0]["database"]

    
    database_name = database_name.replace(".","")

    if database.disable_database(database_name, task[0]["server"]):
        print("Error with disabling database")
        sys.exit(-1)   
    else:
        print("Database disabled")
        sys.exit(0)

#print(database.find_virtual_database('pipedb'))

# if database.delete_bookmark('dupa3') == 0:
#     print("OK")
# else:
#     print("NOT OK")








