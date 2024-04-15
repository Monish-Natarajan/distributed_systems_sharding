import sqlite3

# dictionary of connection objects one for each shard
db_logger_connection = {}

def add_connector(shard_id):
    db_logger_connection[shard_id] = sqlite3.connect(
        database=f"distributed_systems_logger_{shard_id}",
    )

def init_logger(shards_list):
    for shard_id in shards_list:
        db_logger_connection[shard_id] = sqlite3.connect(
            database=f"distributed_systems_logger_{shard_id}",
        )
        cursor = db_logger_connection[shard_id].cursor()
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS logs (
            Serial_No INTEGER PRIMARY KEY AUTOINCREMENT,
            op_type TEXT,
            Stud_id_low INTEGER,
            Stud_id_high INTEGER  )
        """
        try:
            cursor.execute(create_table_query)
            print(f"WAL logger for shard {shard_id} initialized successfully.")
        except sqlite3.Error as er:
            print('SQLite error: %s' % (' '.join(er.args)))
            print("Exception class is: ", er.__class__)
            raise
        cursor.close()


def write_log_entry(shard_id, op_type, stud_id_low, stud_id_high):
    cursor = db_logger_connection[shard_id].cursor()
    insert_query = """
    INSERT INTO logs (op_type, Stud_id_low, Stud_id_high) 
    VALUES (%s, %s, %s);
    """
    values = (op_type, stud_id_low, stud_id_high)
    try:
        cursor.execute(insert_query, values)
        print("WAL log written successfully.")
    except sqlite3.Error as er:
        print('SQLite error: %s' % (' '.join(er.args)))
        print("Exception class is: ", er.__class__)
    finally:
        cursor.close()


def commit_logs(shard_id):
    try:
        db_logger_connection[shard_id].commit()
        print("WAL log committed successfully.")
    except sqlite3.Error as er:
        print('SQLite error: %s' % (' '.join(er.args)))
        print("Exception class is: ", er.__class__)