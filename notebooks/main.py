import requests
import json

request = requests.get('https://api.themoviedb.org/3/movie/550?api_key=9a678b67bfed6aa1fbc96f93b5c618f5')

dados = json.loads(request.content)

out_file = open("imdb.json", "w")  
json.dump(dados, out_file, indent = 6)  
out_file.close() 