import sqlite3
# dictionary of connection objects one for each shard
db_logger_connection = {}

def add_connector(shard_id):
    if shard_id in db_logger_connection:
        db_logger_connection[shard_id].close()
    db_logger_connection[shard_id] = sqlite3.connect(
        database=f"distributed_systems_logger_{shard_id}.db",
    )

def init_logger(shards_list):
    for shard_id in shards_list:
        db_logger_connection[shard_id] = sqlite3.connect(
            database=f"distributed_systems_logger_{shard_id}.db",
        )
        cursor = db_logger_connection[shard_id].cursor()
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS logs (
            Serial_No INTEGER PRIMARY KEY AUTOINCREMENT,
            op_type TEXT,
            num_records INTEGER,
            json_data TEXT)
        """
        try:
            cursor.execute(create_table_query)
            print(f"WAL logger for shard {shard_id} initialized successfully.")
        except sqlite3.Error as er:
            print('SQLite error: %s' % (' '.join(er.args)))
            print("Exception class is: ", er.__class__)
            raise
        db_logger_connection[shard_id].commit()
        cursor.close()


def write_log_entry(shard_id, op_type, num_records, json_data):
    cursor = db_logger_connection[shard_id].cursor()
    insert_query = """
    INSERT INTO logs (op_type, num_records, json_data) 
    VALUES (?, ?, ?);
    """
    values = (op_type, num_records, json_data)
    try:
        cursor.execute(insert_query, values)
        print("WAL log written successfully.")
    except sqlite3.Error as er:
        print('SQLite error: %s' % (' '.join(er.args)))
        print("Exception class is: ", er.__class__)
    finally:
        commit_logs(shard_id) # flush changes to disk
        cursor.close()

def get_logs(shard_id):
    # each log entry in the table "logs" has the format (op_type, num_records, json_data) 
    # where op_type is the operation type (write, update, delete) and num_records is the number of records affected
    # json_data is the actual data that was written, updated or deleted, depending on the request type
    try:
        cursor = db_logger_connection[shard_id].cursor()
        cursor.execute("SELECT * FROM logs")
        logs = cursor.fetchall()
        cursor.close()
    except sqlite3.Error as er:
        print('SQLite error: %s' % (' '.join(er.args)))
        print("Exception class is: ", er.__class__)
        raise er
    return logs

def commit_logs(shard_id):
    try:
        db_logger_connection[shard_id].commit()
        print("WAL log committed successfully.")
    except sqlite3.Error as er:
        print('SQLite error: %s' % (' '.join(er.args)))
        print("Exception class is: ", er.__class__)

def count_log_entries(shard_id):
    try:
        cursor = db_logger_connection[shard_id].cursor()
        # sum up the num_records column to get the total number of log entries
        cursor.execute("SELECT SUM(num_records) FROM logs")
        count = cursor.fetchone()[0]
        cursor.close()
        if count is None:
            return 0
        return count
    except sqlite3.Error as er:
        print('SQLite error: %s' % (' '.join(er.args)))
        print("Exception class is: ", er.__class__)
        raise er