# table-watcher
watch tables in a database for insertion, deletion and updates and truncation. 

## How it works?
provide a database and watch table name to the module. after every operation, an entry will be added to the watch table with a timestamp.This table can be queried to see which tables were modified. The watch table has to be created before being passed to the module and must contain the following columns

1. **table_name**
2. **timestamp**

