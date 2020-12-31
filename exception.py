# import threading

# lock = threading.Lock()

# def meth1():
#     with lock:
#         print("meth1")

# def meth2():
#     with lock:
#         print("meth2 begin")
#         meth1()
#         print("meth2 end")


# meth2()

from kvs import KeyValueStore

store = KeyValueStore.open('haris')

while True:
    print("c-> Create")
    print("r-> Read")
    print("d-> Delete")

    choice = input()

    if choice == 'c':
        key, value, ttl = input().split()
        value = {
            'value': value
        }
        try:
            store.create(key, value, ttl)
        except Exception as e:
            print(e)
        
    elif choice == 'r':
        key = input()
        try:
            value = store.read(key)
            print(value)

        except Exception as e:
            print(e)
    
    elif choice == 'd':
        key = input()
        try:
            store.delete(key)
        except Exception as e:
            print(e)