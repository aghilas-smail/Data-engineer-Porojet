import os

# Param Api

Path = "./data/last_processed.json"
Limit = 100
offsetlimit = 10000 - 1 - Limit


# 3 Parametres to put in the URL of the API: Max_limit, date and offset/

URL_API = "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/rappelconso0/records?limit={}&where=date_de_publication%20%3E%20'{}'&order_by=date_de_publication%20DESC&offset={}"
URL_API = URL_API.format(Limit, "{}","{}")


# PostrSQL Paramet 

user_name = os.getenv("Postgres_user", "localhost")
Postgres_URL = f"jdbc:postgresql://{user_name}:5432/postgres"
Postgres_Properties = {
    "user": "postrges",
    "password": os.getenv("Postgres_Password"),
    "driver": "org.postgresql.Driver"
}