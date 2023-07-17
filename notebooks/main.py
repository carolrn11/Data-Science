import requests
import pyodbc
from tqdm import tqdm
from config import API_KEY, server, database, username, password


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
            created_at = review["created_at"]
            author_name = author_details.get("author_name")
            author_username = author_details.get("author_username")
            author_rating = author_details.get("author_rating")

            cursor.execute("SELECT * FROM MovieReviews WHERE Author = ? AND MovieID = ?", (author, movie_id))
            existing_review = cursor.fetchone()

            if existing_review is None:
                try:
                    cursor.execute("INSERT INTO MovieReviews (MovieID, Author, CreatedAt, AuthorRating) VALUES (?, ?, ?, ?)",
                                   (movie_id, author, created_at, author_rating))
                except pyodbc.IntegrityError as e:
                    pass
                
                cursor.execute("SELECT * FROM Authors WHERE AuthorName = ?", (author_name,))
                existing_author = cursor.fetchone()

                if existing_author is None:
                    cursor.execute("INSERT INTO Authors (AuthorName, AuthorUsername) VALUES (?, ?)",
                                   (author_name, author_username))
                else:
                    cursor.execute("UPDATE Authors SET AuthorUsername = ? WHERE AuthorName = ?",
                                   (author_username,  author_name))

            else:
               
                cursor.execute("UPDATE MovieReviews SET CreatedAt = ?, AuthorRating = ? WHERE Author = ? AND MovieID = ?",
                               (created_at, author_rating, author, movie_id))
                
                cursor.execute("SELECT * FROM Authors WHERE AuthorName = ?", (author_name,))
                existing_author = cursor.fetchone()

                if existing_author is None:
                    cursor.execute("INSERT INTO Authors (AuthorName, AuthorUsername) VALUES (?, ?)",
                                   (author_name, author_username))
                else:
                    cursor.execute("UPDATE Authors SET AuthorUsername = ? WHERE AuthorName = ?",
                                   (author_username, author_name))

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

        genre_names = [genre["name"] for genre in details.get("genres", [])]
        genres = ", ".join(genre_names)

        production_companies_names = [production_companies["name"] for production_companies in details.get("production_companies", [])]
        production_companies = ", ".join(production_companies_names)

        content = details.get('content', None)

        movie_data = (
            movie_id,
            details.get('title', '').replace("'", "''"),
            details.get('release_date', None),
            details.get('vote_average', None),
            details.get('vote_count', None),
            details.get('popularity', None),
            details.get('budget', None),
            details.get('runtime', None),
            details.get('poster_path', None),
            genres.replace("'", "''"),
            production_companies.replace("'", "''")
            
        )
        cursor.execute("UPDATE Movies SET content = {content} WHERE id = {movie_id}")
        cnxn.commit()


def update_tables(reviews=True, movies=True):
    if reviews==True:
        insert_reviews_for_popular_movies()
        print("Reviews inseridos")

    if movies==True:
        print("Inserindo filmes na tabela...")
        insert_details_for_popular_movies()

update_tables(reviews=False, movies=True)

