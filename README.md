# table-watcher
watch tables in a database for insertion, deletion and updates and truncation. 

##How it works?
provide a database and watch table name to the module. after every operation, an entry will be added to the watch table with a timestamp.
this table can be queried to see which tables were modified.

