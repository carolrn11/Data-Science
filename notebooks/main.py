import requests
import pyodbc
from tqdm import tqdm

API_KEY = '9a678b67bfed6aa1fbc96f93b5c618f5'

server = "LAPTOP-KL74V0IB"
database = "ONBOARDING"
username = "sa"
password = "dson4d"

cnxn = pyodbc.connect(f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}")
cursor = cnxn.cursor()


def get_popular_movies():
    print("Pegando ids dos filmes...")
    endpoint = "movie/popular"
    movie_ids = []

    for page in range(1, 500):
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

def extract_author_details(review):
    author_details = review.get("author_details", {})
    return {
        "author_name": author_details.get("name"),
        "author_username": author_details.get("username"),
        "author_rating": author_details.get("rating"),
    }

def insert_reviews_for_popular_movies():
    popular_movie_ids = get_popular_movies()
    print("Inserindo os reviews no banco...")

    for movie_id in tqdm(popular_movie_ids):
        reviews = get_reviews(movie_id)
        

        for review in reviews:
            author_details = extract_author_details(review)
            author = review["author"]
            content = review["content"]
            created_at = review["created_at"]
            author_name = author_details.get("author_name")
            author_username = author_details.get("author_username")
            author_rating = author_details.get("author_rating")

            # Verificar se já existe um registro com o mesmo ID do filme e nome do autor
            cursor.execute(f"SELECT * FROM MovieReviews WHERE Author = ? AND MovieID = {movie_id}", (author))
            result = cursor.fetchone()

            if result is None:
                # Se não houver registro, inserir o comentário na tabela
                cursor.execute("INSERT INTO MovieReviews (MovieID, Author, Content, CreatedAt, AuthorName, AuthorUsername, AuthorRating) VALUES (?, ?, ?, ?, ?, ?, ?)",
                               (movie_id, author, content, created_at, author_name, author_username, author_rating))
            
            else:
                # Se houver registro, atualizar as informações do autor
                cursor.execute(f"UPDATE MovieReviews SET Content=?, CreatedAt=?, AuthorName=?, AuthorUsername=?, AuthorRating=? WHERE Author=? AND MovieID=?",
                    (content, created_at, author_name, author_username, author_rating, author, movie_id)
                )

                
                cnxn.commit()

def get_details(movie_id):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={API_KEY}&language=en-US"
    response = requests.get(url)
    details = response.json()
    return details

def insert_details_for_popular_movies():
    popular_movie_ids = get_popular_movies()
    
    for movie_id in popular_movie_ids:
        details = get_details(movie_id)

        movie_data = (
            movie_id,
            details.get('title', '').replace("'", "''"),
            details.get('overview', '').replace("'", "''"),
            details.get('release_date', None),
            details.get('vote_average', None),
            details.get('vote_count', None),
            details.get('popularity', None)
        )

        # Verificar se já existe um registro com o mesmo ID do filme e nome do autor
        cursor.execute(f"SELECT * FROM Movies WHERE id = {movie_id}")
        result = cursor.fetchone()

        if result is None:
            # Se não houver registro, inserir o comentário na tabela
            cursor.execute("INSERT INTO Movies (id, title, overview, releasedate, voteaverage, votecount, popularity) VALUES (?, ?, ?, ?, ?, ?, ?)", movie_data)
            cnxn.commit()

def update_tables(reviews=True, movies=True):
    if reviews==True:
        insert_reviews_for_popular_movies()
        print("Reviews inseridos")

    if movies==True:
        print("Inserindo filmes na tabela...")
        insert_details_for_popular_movies()

update_tables(reviews=True, movies=False)

