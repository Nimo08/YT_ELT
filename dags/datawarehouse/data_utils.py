from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

table = "yt_api"


# Connect to postgress db
def get_conn_cursor():
    # Set the postgres hook
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
    conn = hook.get_conn()
    # RealDictCursor changes how the data is returned
    # Will return the SQL query as a python dict instead of the default tuple
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur


# Close cursor & conn objects when done to release resources
def close_conn_cursor(conn,cur):
    cur.close()
    conn.close()

# Create schema
def create_schema(schema):
    conn, cur = get_conn_cursor()

    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"

    # Run the sql
    cur.execute(schema_sql)

    # Commit changes to the database
    conn.commit()

    close_conn_cursor(conn, cur)


# Create table
def create_table(schema):

    conn, cur = get_conn_cursor()

    if schema == "staging":
        table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" VARCHAR(20) NOT NULL,
                    "Video_Views" INT,
                    "Likes_Count" INT,
                    "Comments_Count" INT   
                );
            """
    else:
        table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" TIME NOT NULL,
                    "Video_Type" VARCHAR(10) NOT NULL,
                    "Video_Views" INT,
                    "Likes_Count" INT,
                    "Comments_Count" INT    
                ); 
            """
    
    # Run the sql
    cur.execute(table_sql)

    # Commit changes to the database
    conn.commit()

    close_conn_cursor(conn, cur)


def get_video_ids(cur, schema):
    # Get all video ids in the table
    cur.execute(f"""SELECT Video_ID FROM {schema}.{table};""")
    # Give list of dictionaries where key: video_id, value: video_id value
    ids = cur.fetchall()
    # Extracting value for each row inside the video ID key
    video_ids = [row["Video_ID"] for row in ids]

    return video_ids