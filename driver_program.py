from kvs import KeyValueStore


store = KeyValueStore.open('haris')

while True:
    print("c-> Create")
    print("r-> Read")
    print("d-> Delete")

    choice = input()

    if choice == 'c':
        print("Enter key, value(only a string and not JSON object), and ttl (space separated)")
        key, value, ttl = input().split()
        value = {
            'value': value
        }
        try:
            store.create(key, value, ttl)
        except Exception as e:
            print(e)
        
    elif choice == 'r':
        print("Enter key")
        key = input()
        try:
            value = store.read(key)
            print(value)

        except Exception as e:
            print(e)
    
    elif choice == 'd':
        print("Enter key")
        key = input()
        try:
            store.delete(key)
        except Exception as e:
            print(e)