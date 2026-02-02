
import os

from dotenv import load_dotenv

import psycopg2
#Explicity load .env from current directory

load_dotenv(dotenv_path=".env")
database_url = os.getenv("DATABASE_URL")
if not database_url:
    raise ValueError("DATABASE_URL not found in .env")
print("Connecting to Neon...")
conn = psycopg2.connect(database_url)
cur = conn.cursor()
cur.execute("SELECT current_database(), current_user;")
result = cur.fetchone()
print("Connected successfully.")
print("Database:", result[0])
print("User:", result[1])
cur.close()
conn.close()
print("Connection closed.")
