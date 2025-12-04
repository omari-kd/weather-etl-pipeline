import yaml
import psycopg2

# Read configuration values from config.yaml (APIURL &  DB credentials)
def load_config(path="../config/config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

# Open a secure SSL PostgreSQL connecttion to your neon database
def get_db_connection(config):
    db = config["database"]
    conn = psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname=db["dbname"],
        user=db["user"],
        password=db["password"],
        sslmode=db["sslmode"]
    )
    return conn