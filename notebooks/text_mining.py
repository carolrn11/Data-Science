import pyodbc
import pandas as pd
from textblob import TextBlob

# Conectar ao SQL Server
server = "LAPTOP-KL74V0IB"
database = "ONBOARDING"
username = "sa"
password = "dson4d"

cnxn = pyodbc.connect(f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}")
cursor = cnxn.cursor()

reviews_query = "SELECT * FROM MovieReviews"
df_reviews = pd.read_sql_query(reviews_query, cnxn)

stop_words = ["the", "a", "an", "and", "of", "to", "in", "for", "on", "at", "with", "by", "from"]
df_reviews["Content"] = df_reviews["Content"].apply(lambda x: " ".join([word.lower() for word in x.split() if word.lower() not in stop_words]))

df_reviews["Polarity"] = df_reviews["Content"].apply(lambda x: TextBlob(x).sentiment.polarity)

for index, row in df_reviews.iterrows():
    cursor.execute(f"UPDATE MovieReviews SET Polarity = ? WHERE MovieID = ? AND Author = ?", (row["Polarity"], row["MovieID"], row["Author"]))
cnxn.commit()

def recommend_movies(movie_id):
    # Selecionar todos os reviews com polaridade positiva para o filme
    cursor.execute(f"SELECT * FROM MovieReviews WHERE MovieID = ? AND Polarity > 0", (movie_id,))
    reviews = cursor.fetchall()

    # Calcular a média de polaridade
    if len(reviews) > 0:
        polarities = [review.Polarity for review in reviews]
        average_polarity = sum(polarities) / len(polarities)
    else:
        average_polarity = 0


recommend_movies(76600)
