# File based key-value store

This file-based key-value data store supports the basic CRD (create, read, and delete) operations. This data store is meant to be used as a local storage for one single process on one laptop. The data store is exposed as a python file named kvs.py.
# Compatibility
Most operating systems should be compatibile as no operating system dependent library is used. However, it was developed on windows and is also tested only on windows.

# Usage
## Importing
> from kvs import KeyValueStore

## Instantiating an object
>store = KeyValueStore.open(filename)
>or
>store = KeyValueStore.open(filename, fileaddress)

**filename** is the name of the file where the data will be stored
**fileaddress** is an optional parameter which is the address of the folder where you want the storage file to be stored. If not provided, the storage file will be stored in the **default directory** which will be discussed later.
Note that if the given file exists, it opens it otherwise, it creates a new file but the given directory (if provided) must be a valid existing directory.
**KeyValueStore's objects MUST NEVER be created by using the default constructor**
## Create
>store.create(key, value)
>or
>store.create(key, value, ttl)

**key** must be a string
**value** must be a JSON object
Throws exception if the key already exists or the storage file is at capacity.
**ttl (Time To Live)** is an optional parameter whose value must be a positive integer. If provided, it causes the key to be automatically deleted after ttl seconds. If not provided, the key lives in the storage until deleted intentionally.
## Read
> store.read(key)

**Returns** a JSON object corresponding to the given key.
Throws exception if the key doesn't exist.
## Delete
> store.delete(key)

Throws an exception if the key doesn't exist.
## Optimize
>store.optimize()

It is an optional method which optimizes the storage file to take up minimum space. It is a quite heavy operation as it requires the whole file to be copied and then built again. The user can choose not to ever use it. The implications of not using it are that there may be possible testcases where the storage file is at its maximum capicity but rarely have any keys in it or doesn't have that many keys. Regardless, the user should be able to insert keys almost fine.

# Features
1. A client process is allowed to access the same storage file with multiple threads and the library takes care of concurrency control. In other words, the data store is thread-safe.
2. Two or more different processes are not allowed to access the same file storage as a datastore. The process tries to access an already busy storage file will get an exception.

# Tweakable Properties
The data store provides some constants which can be tweaked to give different performance/constraints according to different requirements.
These properties can be tweaked by opening **kvs.py** and changing the values of the constants defined in the beginning of the class KeyValueStore.
1. **_DEFAULT_DIRECTORY**: If no file_address is provided while opening a storage file, the file will be searched/created in this directory. By default, it is empty and so all storage files are created in the same directory where kvs.py resides unless otherwise specified by using the file_address variable.
2. **_DB_SIZE_LIMIT**: The maximum size of the storage file allowed. 1 GB by default.
3. **_KEY_SIZE_LIMIT**: The maximum size of the key that is acceptable. 32 characters by default.
4. **_VALUE_SIZE_LIMIT**: The maximum size of the JSON object (the value of the key) that is acceptable. 16 KB by default. The size is measured by converting the JSON object into string.
5. **_MAX_UNCOMMITTED_TRANSACTIONS_ALLOWED**: Maximum create or delete operations which may remain uncommitted at any particular time. 10,000 by default. Note that all operations are always committed whenever the object is destroyed and at some other times also as discussed later.
6. **_MAX_UNCOMMITTED_SIZE_ALLOWED**: Maximum size of sum of all uncommitted values allowed. 15 MB by default.
7. **_PERIODIC_COMMIT_TIME**: The maximum time before which all the uncommitted create or delete operations must be committed.  120 seconds by default

# Exceptions
All the exceptions are raised by using Exception class with first argument as the Exception meaning and second argument as teh exception code.

Exception code  | Exception meaning
------------- | -------------
100  | Some other process is already accessing the desired file
101  | Size of key must not be more than the specified size
102 | Size of the value must not more than the specified size
103 | Can't store any more keys because file is already at its maximum capacity
201 | The given key already exists
202 | The given key does not exist

# Testing
Some major unit tests are present in test_kvs.py. 
**Note: Before running unit tests,
Please make sure that _DEFAULT_DIRECTORY is set to empty string before testing, and
Delete any file named 'test' (if exists) in the directory where kvs.py exists**
Please check test_kvs.py to get more information about what tests are provided.

# Other files
## driver_program.py
It contains a basic driver program which lets the user create, read, and delete keys.

##  another_process_for_testing.py
It contains code which simulates a different process trying to access a storage file. This file is required for the tests to be executed successfully. This file is not used anywhere other than test_kvs.py.