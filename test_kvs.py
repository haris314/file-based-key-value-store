import unittest
from kvs import KeyValueStore
import random
import time
import threading
import subprocess
import json
from pathlib import Path


class TestKVS(unittest.TestCase):

    def test_create_read(self):
        """
        Creates a key and retrieves and checks that both the values are equal
        """
        store = KeyValueStore.open('test')
        key, value = TestKVS.get_new_key_value(store)

        store.create(key, value)
        returned_value = store.read(key)
        self.assertEqual(value, returned_value)

    def test_delete(self):
        """
        Creates a key
        Deletes it
        Checks that the store raises exception on reading the key
        """
        store = KeyValueStore.open('test')
        key, value = TestKVS.get_new_key_value(store)

        store.create(key, value)
        store.delete(key)
        self.assertRaises(Exception, store.read, key)

    def test_ttl_read(self):
        """
        Creates a key with ttl = 1
        Waits for two seconds
        And checks that the key is gone by reading it
        """
        store = KeyValueStore.open('test')
        key, value = TestKVS.get_new_key_value(store)

        store.create(key, value, 1)
        time.sleep(2)
        self.assertRaises(Exception, store.read, key)

    def test_ttl_delete(self):
        """
        Creates a key with ttl = 1
        Waits for two seconds
        And checks that the key is already gone by deleting it
        """
        store = KeyValueStore.open('test')
        key, value = TestKVS.get_new_key_value(store)

        store.create(key, value, 1)
        time.sleep(2)
        self.assertRaises(Exception, store.delete, key)

    def test_multi_threading(self):
        """
        Creates two threads
        Both the threads accesses the same data store
        One thread creates a key, the other thread tries to read the key and asserts the value
        """
        store = KeyValueStore.open('test')
        key, value = TestKVS.get_new_key_value(store)

        create_thread = threading.Thread(target=TestKVS.create_key_and_wait, args=(key, value,))
        read_thread = threading.Thread(target=self.wait_and_read_key, args=(key, value,))

        create_thread.start()
        read_thread.start()

    def test_another_process(self):
        """
        Opens a data store and makes the other process open the same data store
        Checks if we get the exception
        """
        print('Testing another process access on same file')
        store = KeyValueStore.open('test')

        output = subprocess.check_output("python another_process_for_testing.py", shell=True).decode('utf-8')
        print('Other process said: ' + output)

        # Assert that the exception code is as expected
        self.assertEqual(100, int(json.loads(output)['Exception code']))

    def test_file_size_limit(self):
        """
        Keeps inserting new keys into the database until the database reaches its limit
        When no more keys are allowed to be inserted anymore, it checks the size of the database
        It deletes all the keys before returning so other tests can run smoothly
        """
        # Reduce to maximum limit so the test can execute efficiently
        KeyValueStore._DB_SIZE_LIMIT = 30 * 1024 * 1024  # 30 Mega Bytes
        store = KeyValueStore.open('test')
        keys = []  # To keep track of the keys to delete later

        while True:
            key, value = TestKVS.get_new_key_value(store)
            try:
                store.create(key, value)
                keys.append(key)
            except Exception as e:
                size = TestKVS.get_test_db_size()
                db_limit = KeyValueStore._DB_SIZE_LIMIT
                expression = .95 * db_limit <= size <= 1.05 * db_limit
                self.assertTrue(expression)

                # Delete the keys so other tests can run
                deleted = 0
                for key in keys:
                    store.delete(key)
                    deleted += 1
                print("deleted " + str(deleted) + " keys")
                return

    def wait_and_read_key(self, key, value):
        """Waits for half a second and reads the given key and asserts the value"""
        store = KeyValueStore.open('test')
        time.sleep(0.5)
        self.assertEqual(value, store.read(key))

    @classmethod
    def create_key_and_wait(cls, key, value):
        """Creates a key and waits for 1 second"""
        store = KeyValueStore.open('test')
        store.create(key, value)
        time.sleep(1)

    @classmethod
    def get_new_key_value(cls, store):
        """
        :param store: A KeyValueStore object
        :return: A key(string) and a value(json) which doesn't exist in the store
        """
        value = {
            'first_field': 'first_value_first_value_first_value_first_value_first_value_first_value_',
            'second_field': 'second_value_second_value_second_value_second_value_second_value_second_value_',
            'third_field': 'third_value_third_value_third_value_third_value_third_value_third_value_third_value_'
        }
        while True:
            key = str(random.randrange(0, 1e18))
            try:
                store.read(key)
            except Exception as e:
                if e.args[1] == 202:
                    return key, value

    @classmethod
    def get_test_db_size(cls):
        """Returns the size of test database in bytes"""
        return Path('test').stat().st_size

