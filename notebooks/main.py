import requests
import pyodbc

API_KEY = '9a678b67bfed6aa1fbc96f93b5c618f5'

server = "LAPTOP-KL74V0IB"
database = "ONBOARDING"
username = "sa"
password = "dson4d"

cnxn = pyodbc.connect(f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}")
cursor = cnxn.cursor()


def get_popular_movies():
    endpoint = "movie/popular"
    movie_ids = []

    for page in range(1, 100):
        url = f"https://api.themoviedb.org/3/{endpoint}?api_key={API_KEY}&language=en-US&page={page}"
        response = requests.get(url)

        for movie in response.json()["results"]:
            movie_ids.append(movie["id"])

    return movie_ids

def get_data_from_tmdb(endpoint, params=None):
    url = f"https://api.themoviedb.org/3/{endpoint}?api_key={API_KEY}"
    if params:
        url += f"&{params}"
    response = requests.get(url)
    return response.json()

def get_reviews(movie_id):
    endpoint = f"movie/{movie_id}/reviews"
    response = get_data_from_tmdb(endpoint)
    reviews = response["results"]
    return reviews

def insert_reviews_for_popular_movies():
    print("Pegando o id dos 2000 filmes mais populares...")
    popular_movie_ids = get_popular_movies()
    print("Inserindo os reviews no banco...")

    for movie_id in popular_movie_ids:
        reviews = get_reviews(movie_id)

        for review in reviews:
            author = review["author"]
            content = review["content"]
            created_at = review["created_at"]

            # Verificar se já existe um registro com o mesmo ID do filme e nome do autor
            cursor.execute(f"SELECT * FROM MovieReviews WHERE Author = ? AND MovieID = {movie_id}", (author))
            result = cursor.fetchone()

            if result is None:
                # Se não houver registro, inserir o comentário na tabela
                cursor.execute(f"INSERT INTO MovieReviews (MovieID, Author, Content, CreatedAt) VALUES (?, ?, ?, ?)",
                    (movie_id, author, content, created_at)
                )
                cnxn.commit()

insert_reviews_for_popular_movies()