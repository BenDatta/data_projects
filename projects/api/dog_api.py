import requests
import pandas as pd

url  = "https://dog.ceo/api/breeds/list/all"

response = requests.get(url)
if response.status_code == 200:
    print("API is working ")
else:
    print(response.status_code)
data = response.json()
print("Data is saved to json file")