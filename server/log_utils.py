import sqlite3

db_logger_connection = sqlite3.connect(
    database="distributed_systems_logger",
)

def init_logger(shards_list):
    cursor = db_logger_connection.cursor()
    for shard_id in shards_list:
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {shard_id}_logs (
            Serial_No INT AUTO_INCREMENT PRIMARY KEY,
            op_type VARCHAR(255),
            Stud_id_low INT,
            Stud_id_high INT  )
        """
        try:
            cursor.execute(create_table_query)
            print("WAL logger initialized successfully.")
        except sqlite3.Error as er:
            print('SQLite error: %s' % (' '.join(er.args)))
            print("Exception class is: ", er.__class__)
            raise
    cursor.close()


def write_log_entry(shard_id, op_type, stud_id_low, stud_id_high):
    cursor = db_logger_connection.cursor()
    insert_query = """
    INSERT INTO %s_logs (op_type, Stud_id_low, Stud_id_high) 
    VALUES (%s, %s, %s);
    """
    values = (shard_id, op_type, stud_id_low, stud_id_high)
    try:
        cursor.execute(insert_query, values)
        print("WAL log written successfully.")
    except sqlite3.Error as er:
        print('SQLite error: %s' % (' '.join(er.args)))
        print("Exception class is: ", er.__class__)
    finally:
        cursor.close()


def commit_logs():
    try:
        db_logger_connection.commit()
        print("WAL log committed successfully.")
    except sqlite3.Error as er:
        print('SQLite error: %s' % (' '.join(er.args)))
        print("Exception class is: ", er.__class__)