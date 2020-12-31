import sqlite3
import threading
import json
import time
from filelock import Timeout, FileLock
import random


class KeyValueStore:

    _DEFAULT_DIRECTORY = ''  # 'C:\\Users\\hrsha\\Documents\\kvs\\'
    _DB_SIZE_LIMIT = 1024 * 1024 * 1024  # Bytes
    _KEY_SIZE_LIMIT = 32  # Chars
    _VALUE_SIZE_LIMIT = 16 * 1024  # Bytes
    _MAX_UNCOMMITTED_TRANSACTIONS_ALLOWED = 10000
    _MAX_UNCOMMITTED_SIZE_ALLOWED = 15 * 1024 * 1025  # Bytes
    _PERIODIC_COMMIT_TIME = 2 * 60  # Seconds

    _all_objects = {}

    _class_lock = threading.Lock()

    def __init__(self, conn, system_lock):
        """
        :param conn: The connection to the sqlite database
        :param system_lock: A FileLock object to the lock file of the database file

        Note: Users must never use this to initialize the objects.
        Use KeyValueStore.open() to initialize the objects.

        - Initializes some variables
        - Starts the periodic commit thread
        """
        self._conn = conn
        self._lock = threading.Lock()
        self._system_lock = system_lock
        self._uncommitted_transactions = 0
        self._uncommitted_size = 0

        # Start the thread to periodically commit
        periodic_commit_thread = threading.Thread(target=self._periodic_commit)
        periodic_commit_thread.start()
        # Start the thread to periodically delete expired keys
        periodic_check_for_ttl = threading.Thread(target=self._periodic_check_all_for_ttl)
        periodic_check_for_ttl.start()

    def __del__(self):
        """
        Destructor
        - Commits any uncommitted transactions and closes the connection to the database
        - Releases the system level lock for the database file
        """
        if self._conn:
            self._conn.commit()
            self._conn.close()
        if self._system_lock:
            self._system_lock.release()

    @classmethod
    def open(cls, file_name, file_directory=None):
        """
        :param file_name: A string for the name of the file
        :param file_directory: A string describing the directory where the file resides
            or must reside in case if the file doesn't exist
        :return: A KeyValueStore Object

        If file_address is not provided, the file will be created at the DEFAULT_DIRECTORY

        """
        # Generate the file address
        if file_directory:
            file_address = file_directory + file_name
        else:
            file_address = cls._DEFAULT_DIRECTORY + file_name

        # Get the class lock
        with cls._class_lock:

            # See if an object already exists which is connected to this database
            if file_address in cls._all_objects:
                return cls._all_objects[file_address]

            # Check if the database exists and create one if it doesn't
            conn = sqlite3.connect(file_address, check_same_thread=False)

            # Create table if doesn't exist
            cls._create_table(conn)

            # Acquire the system lock
            lock_address = file_address + '.lock'
            system_lock = FileLock(lock_address)
            try:
                system_lock.acquire(timeout=1)
            except Timeout:
                raise Exception("Some other process is already accessing the desired file", 10)

            # Create the object
            new_obj = KeyValueStore(conn, system_lock)
            cls._all_objects[file_address] = new_obj

            return new_obj

    def create(self, key, value, ttl=-1):
        """
        :param key: A string for the key which should be capped at 32 chars
        :param value: A JSON object which should be smaller than 16KB
        :param ttl: An integer defining the Time To Live in seconds. -1 means infinite
        :return: Void
        """
        key_len = len(key)
        string_value = json.dumps(value)
        value_len = len(string_value)

        # Check if the key is oversized
        if key_len > KeyValueStore._KEY_SIZE_LIMIT:
            raise Exception('Size of key must not be more than ' + str(KeyValueStore._KEY_SIZE_LIMIT) + ' chars', 101)

        # Check if the value is oversized
        if value_len >= KeyValueStore._VALUE_SIZE_LIMIT:
            raise Exception('Size of the value must not more than ' + str(KeyValueStore._VALUE_SIZE_LIMIT) + ' bytes',
                            102)

        self._check_for_ttl(key)
        cursor = self._conn.cursor()

        # Get lock
        with self._lock:

            # Check if the key already exists
            result = cursor.execute(f"SELECT * FROM key_value_store WHERE key = '{key}'").fetchone()
            if result:
                raise Exception("Given key already exists", 201)

            # Check if the database has already exceeded the _DB_SIZE_LIMIT
            if KeyValueStore._db_size(self._conn) >= KeyValueStore._DB_SIZE_LIMIT:
                raise Exception("Can't store any more keys because file is already at its maximum capacity", 103)

            # Put the key in the database
            cursor.execute(f"INSERT INTO key_value_store \
                VALUES ('{key}', '{string_value}', '{time.time()}', '{ttl}')")

        self._uncommitted_size += key_len + value_len
        self._commit()

    def read(self, key):
        """
        :param key: A string representing the key
        :return: A JSON object which is the value of the given key in the store
        """

        self._check_for_ttl(key)

        # Get the record from the database
        cursor = self._conn.cursor()
        record = cursor.execute(f"SELECT * FROM key_value_store WHERE key = '{key}'").fetchone()

        # If the given key doesn't exist
        if not record:
            raise Exception("The given key does not exist", 202)

        # Return the JSON value corresponding to the key
        return json.loads(record[1])

    def delete(self, key):
        """
        :param key: A string representing the key
        :return: Void
        """

        self._check_for_ttl(key)
        cursor = self._conn.cursor()

        with self._lock:
            # Get the record from the database
            record = cursor.execute(f"SELECT * FROM key_value_store WHERE key = '{key}'").fetchone()

            # If there is no value with the given key
            if not record:
                raise Exception("The given  key does not exist", 202)

            # Delete the record
            cursor.execute(f"DELETE FROM key_value_store WHERE key = '{key}'")

        self._commit()

    def optimize_file(self):
        """
        :return: Void
        Optimizes the file to take minimal space on disk
        It is a heavy operation and no queries will be processed until it is finished
        """
        self._conn.execute(f"VACUUM")  # Rebuilds the database

    @classmethod
    def _create_table(cls, conn):
        """
        :param conn: Connection to the database
        :return: Void
        Creates the table which is to be used to store the key value store
        Only creates if the table doesn't exist, otherwise has not effect
        """
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS key_value_store (
            key TEXT PRIMARY KEY, 
            value TEXT,
            timestamp FLOAT,
            ttl INTEGER
        ) 
        """)
        conn.commit()

    @classmethod
    def _db_size(cls, conn):
        """
        :return: The size (in bytes) of the database that this object is connected to
        """
        page_count = conn.execute('PRAGMA PAGE_COUNT').fetchone()[0]
        page_size = conn.execute('PRAGMA PAGE_SIZE').fetchone()[0]
        return page_count * page_size

    def _commit(self):
        """
        :return: Void
        Commits the transactions if
            - number of uncommitted transactions has reached the limit, or
            - size of uncommitted transactions has reached the limit
        """
        self._uncommitted_transactions += 1
        if self._uncommitted_transactions >= KeyValueStore._MAX_UNCOMMITTED_TRANSACTIONS_ALLOWED\
                or self._uncommitted_size >= KeyValueStore._MAX_UNCOMMITTED_SIZE_ALLOWED:

            with self._lock:
                self._conn.commit()

                self._uncommitted_transactions = 0
                self._uncommitted_size = 0

    def _periodic_commit(self):
        """
        :return:
        Commits all the transactions before every _PERIODIC_COMMIT_TIME seconds
        """
        while True:
            with self._lock:)
                self._conn.commit()

            # Sleep for some random time
            time.sleep(KeyValueStore._PERIODIC_COMMIT_TIME/2
                       + random.randrange(0, KeyValueStore._PERIODIC_COMMIT_TIME/2))

    def _check_for_ttl(self, key):
        """
        :param key: A string for the key
        :return: Void
        If the given key exists and is expired, it is deleted
        """
        # Get the lock
        with self._lock:
            # Get the record from the database
            self._conn.execute(f"DELETE FROM key_value_store \
                                       WHERE key={key} \
                                       AND ttl != -1 \
                                       AND {time.time()} - timestamp > ttl")

    def _periodic_check_all_for_ttl(self):
        """
        :return: Void
        Keep deleting all the expired keys
        """
        while True:
            # Sleep for some random time
            time.sleep(KeyValueStore._PERIODIC_COMMIT_TIME / 2
                       + random.randrange(0, KeyValueStore._PERIODIC_COMMIT_TIME / 2))

            # Get the lock
            with self._lock:
                # Delete teh expired keys
                self._conn.execute(f"DELETE FROM key_value_store \
                                   WHERE ttl != -1 \
                                   AND {time.time()} - timestamp > ttl")

    def _debug_print_all_keys(self):
        cursor = self._conn.cursor()
        rows = cursor.execute("SELECT * FROM key_value_store").fetchall()
        for row in rows:
            print(row)

    def _debug_insert_n_keys(self, n, ttl=-1):
        cursor = self._conn.cursor()

        json_object = {
            'first_key': 'first valeh djisf jskadl hfidsnv kdfhjishdn fksdhjif',
            'second_key': 'second_valuej kfdsjf klsdjf klsdjf ksdlfj  '
        }
        for i in range(5):
            json_object[i] = json.loads(json.dumps(json_object))
        print(json_object)
        for i in range(n):
            key = random.randint(0, 1000000000000000000)
            try:
                self.create(str(key), json_object, ttl)
            except Exception as e:
                print(e)


