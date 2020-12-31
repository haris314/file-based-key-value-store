from kvs import KeyValueStore
import json

"""
Only meant for testing
Opens the data store named test that would be generated while tests are being executed
If test is successful, opening test from here should raise an exception
"""
try:
    store = KeyValueStore.open('test')
except Exception as e:
    json_object = {
        "Exception message": e.args[0],
        "Exception code": e.args[1]
    }
    print(json.dumps(json_object))

