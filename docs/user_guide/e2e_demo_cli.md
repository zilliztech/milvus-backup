# E2e Demo

To try this demo, you should have a functional Milvus server installed and have pymilvus library installed.

Step 0: Check the connections

First of all, we can use `check` command to check whether connections to milvus and storage is normal:

```
./milvus-backup check
```

normal output:

```shell
Succeed to connect to milvus and storage.
Milvus version: v2.4.0
Storage:
milvus-bucket: a-bucket
milvus-rootpath: files
backup-bucket: a-bucket
backup-rootpath: backup
```

Step 1: Prepare the Data

Create a collection in Milvus called `hello_milvus` and insert some data using the following command:

```
python example/prepare_data.py
```

Step 2: Create a Backup

Use the following command to create a backup of the `hello_milvus` collection:

```
./milvus-backup create -n my_backup
```

Step 3: Restore the Backup

Restore the backup using the following command:

```
./milvus-backup restore -n my_backup -s _recover
```

This will create a new collection called `hello_milvus_recover` which contains the data from the original collection.

**Note:** if you want to restore index as well, add `--restore_index`, like this:

```
./milvus-backup restore --restore_index -n my_backup -s _recover
```

This will help you restore data and index at the same time. If you don't add this flag, you need to restore index manually.

Step 4: Verify the Restored Data

Create an index on the restored collection using the following command:

```
python example/verify_data.py
```

This will perform a search on the `hello_milvus_recover` collection and verify that the restored data is correct.

That's it! You have successfully backed up and restored your Milvus collection.
