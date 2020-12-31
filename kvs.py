import sqlite3
import threading
import json
import time
from filelock import Timeout, FileLock


class KeyValueStore:

    _DEFAULT_DIRECTORY = ""
    _DB_SIZE_LIMIT = 1024 * 1024 * 1024  # Bytes
    _KEY_SIZE_LIMIT = 32  # Chars
    _VALUE_SIZE_LIMIT = 16 * 1024  # Bytes
    _all_objects = {}

    _class_lock = threading.Lock()

    def __init__(self, conn, system_lock):
        """
        :param conn: The connection to the sqlite database
        :param system_lock: A FileLock object to the lock file of the database file

        Note: Users must never use this to initialize the objects.
        Use KeyValueStore.open() to initialize the objects.
        """
        self._conn = conn
        self._lock = threading.Lock()
        self._system_lock = system_lock

    def __del__(self):
        """
        Destructor
        - Closes the connection to the database
        - Releases the system level lock for the database file
        """
        if self._conn:
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
            try:
                conn = sqlite3.connect(file_address)
            except Exception as e:
                print(e)
                return

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

        # Check if the key is oversized
        if len(key) > KeyValueStore._KEY_SIZE_LIMIT:
            raise Exception('Size of key must not be more than ' + str(KeyValueStore._KEY_SIZE_LIMIT) + ' chars', 101)

        # Check if the value is oversized
        string_value = json.dumps(value)
        if len(string_value) >= KeyValueStore._VALUE_SIZE_LIMIT:
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
                VALUES ('{key}', '{json.dumps(value)}', '{time.time()}', '{ttl}')")

            self._conn.commit()

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
            self._conn.commit()

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
            self._conn.commit()

    def _check_all_for_ttl(self):
        """
        :return: Void
        Deletes all the expired keys and rebuilds the database to occupy minimal amount of disk space
        """
        self._conn.execute(f"DELETE FROM key_value_store \
                           WHERE ttl != -1 \
                           AND {time.time()} - timestamp > ttl")

        self._conn.commit()
        self._conn.execute(f"VACUUM")  # Rebuilds the database

    def _debug_print_all_keys(self):
        cursor = self._conn.cursor()
        rows = cursor.execute("SELECT * FROM key_value_store").fetchall()
        for row in rows:
            print(row)

    def _debug_insert_n_keys(self, n, ttl=-1):
        cursor = self._conn.cursor()
        mx = cursor.execute("SELECT MAX(key) FROM key_value_store").fetchone()[0]
        mx = int(mx) + 1

        json_object = {
            'first_key': 'first valeh djisf jskadl hfidsnv kdfhjishdn fksdhjif',
            'second_key': 'second_valuej kfdsjf klsdjf klsdjf ksdlfj  '
        }
        for i in range(n):
            cursor.execute(f"INSERT INTO key_value_store VALUES( \
            '{mx}', '{json.dumps(json_object)}', {time.time()}, {ttl})")

            mx += 1

        self._conn.commit()

