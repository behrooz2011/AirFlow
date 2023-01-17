import requests
print("hi")
req = requests.request('GET', 'https://httpbin.org/get')

print(req)
print("bye")