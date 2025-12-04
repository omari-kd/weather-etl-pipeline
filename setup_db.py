from src.etl.utils import load_config, get_db_connection

config = load_config("src/config/config.yaml")
conn = get_db_connection(config)
cur = conn.cursor()

with open("src/sql/create_tables.sql") as f:
    cur.execute(f.read())

conn.commit()
conn.close()

print("Database table created successfully")