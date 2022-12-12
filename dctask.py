import os
import sys
import yaml
from yaml.loader import SafeLoader

from dct import common
from dct import database

print("Entry")

common.BASE_URL=os.getenv('URL')
common.API_KEY=os.getenv('API_KEY')
task_name=os.getenv('TASK_NAME')

print("ALAMAKOTA")
print(common.BASE_URL)

with open('/code/dctasks.yml') as f:
   task_list = yaml.load(f, Loader=SafeLoader)


task = [ x for x in task_list if x["name"] == task_name ]

if len(task) != 1:
    print("Task not found or not unique")
    sys.exit(-1)


if task[0]["action"] == "create_bookmark":
    if database.create_bookmark(task[0]["source"], task[0]["bookmark_name"]):
        print("Error with creating bookmark")
        sys.exit(-1)
    else:
        print("bookmark created")
        sys.exit(0)

if task[0]["action"] == "delete_bookmark":
    if database.delete_bookmark(task[0]["bookmark_name"]):
        print("Error with deleting bookmark")
        sys.exit(-1)
    else:
        print("bookmark deleted")
        sys.exit(0)

if task[0]["action"] == "refresh_from_bookmark":
    if database.refresh_vdb(task[0]["database_name"], bookmark_name = task[0]["bookmark_name"]):
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








