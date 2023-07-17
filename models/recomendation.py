import pyodbc
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from config import server, database, username, password


# Conexão com o banco de dados
cnxn = pyodbc.connect(f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}")
cursor = cnxn.cursor()

# Função para obter os perfis dos filmes
def get_movie_profiles():
    cursor.execute("SELECT id, genre FROM Movies")
    rows = cursor.fetchall()
    movie_profiles = {}

    movie_ids = []  # Lista para armazenar os valores da coluna ID

    for row in rows:
        movie_id = row.id
        genre = row.genre

        # Pré-processamento do gênero do filme
        genre = genre.lower().replace(" ", "")

        # Atualização do perfil do filme
        movie_profiles[movie_id] = genre

        # Adicionar o valor da coluna ID à lista movie_ids
        movie_ids.append(movie_id)

    return movie_profiles, movie_ids

# Uso da função get_movie_profiles()
movie_profiles, movie_ids = get_movie_profiles()

# Função para recomendar filmes
def recommend_movies(movie_id, movie_profiles, top_n=5):
    # Obter o perfil do filme de interesse
    movie_profile = movie_profiles.get(movie_id)

    if movie_profile is None:
        print("O filme de interesse não foi encontrado.")
        return

    # Vetorização dos perfis dos filmes
    movie_ids = list(movie_profiles.keys())
    movie_genre_profiles = list(movie_profiles.values())

    vectorizer = CountVectorizer()
    movie_genre_matrix = vectorizer.fit_transform(movie_genre_profiles)

    # Cálculo da similaridade entre o filme de interesse e os demais filmes
    movie_index = movie_ids.index(movie_id)
    similarities = cosine_similarity(movie_genre_matrix[movie_index], movie_genre_matrix).flatten()

    # Obtenção dos índices dos filmes mais similares
    similar_movie_indices = similarities.argsort()[::-1][1:top_n+1]

    # Recomendação dos filmes
    recommended_movies = [movie_ids[index] for index in similar_movie_indices]
    print("Filmes recomendados:", recommended_movies)

    # Supondo que a tabela de recomendações se chame "MovieRecommendations" e tenha as colunas "MovieID" e "RecommendedMovieID"
    for recommended_movie_id in recommended_movies:
        cursor.execute("INSERT INTO MovieRecommendations (MovieID, RecommendedMovieID) VALUES (?, ?)",
                    (movie_id, recommended_movie_id))
        cnxn.commit()

    # return recommended_movies

recommend_movies(movie_ids, movie_profiles, top_n=5)

