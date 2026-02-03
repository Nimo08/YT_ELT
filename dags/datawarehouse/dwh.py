from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor, create_schema, create_table, get_video_ids
from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_transformation import transform_data

import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)
table = "yt_api"

# Create staging table in data warehouse
@task
def staging_table():

    schema = "staging"

    conn, cur = None, None 

    try:
        conn, cur = get_conn_cursor()

        YT_data = load_data()

        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(cur, schema)

        # Insert/update depending on the conditions
        for row in YT_data:
            if len(table_ids) == 0:
                insert_rows(cur, conn, schema, row)
            else:
                if row["video_id"] in table_ids:
                    update_rows(cur,conn,schema,row)
                else:
                    insert_rows(cur,conn,schema,row)
        
        # Set of all video_ids that are present
        id_in_json = {row["video_id"] for row in YT_data}

        # Difference btn video_ids in the staging table and ids_in_json
        ids_to_delete = set(table_ids) - id_in_json

        if ids_to_delete:
            delete_rows(cur,conn,schema,ids_to_delete)
        logger.info(f"{schema} table update completed")
    
    except Exception as e:
        # Log any exceptions that occur
        logger.error(f"An error occurred during the update of {schema} table: {e}")
        raise e
    
    finally:
        # Ensure the connection and cursor are closed
        if conn and cur:
            close_conn_cursor(conn,cur)


@task
def core_table():
    schema = "core"

    conn, cur, = None, None

    try:
        conn, cur = get_conn_cursor()

        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(cur, schema)

        # Collect all video IDs from the current execution, for deletion logic
        current_video_ids = set()

        # Get all data from staging table and fetch rows
        cur.execute(f"SELECT * FROM staging.{table};")
        rows = cur.fetchall()

        for row in rows:
            current_video_ids.add(row["video_id"])

            if len(table_ids) == 0:
                transformed_row = transform_data(row)
                insert_rows(cur, conn, schema, transformed_row)
            
            else:
                transformed_row = transform_data(row)
                if transformed_row["video_id"] in table_ids:
                    update_rows(cur, conn, schema, transformed_row)
                else:
                    insert_rows(cur, conn, schema, transformed_row)
        
        ids_to_delete = set(table_ids) - current_video_ids

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)
        
        logger.info(f"{schema} table update completed")
    
    except Exception as e:
        # Log any exceptions that occur
        logger.error(f"An error occurred during the update of {schema} table: {e}")
        raise e
    
    finally:
        # Ensure the connection and cursor are closed
        if conn and cur:
            close_conn_cursor(conn, cur)
