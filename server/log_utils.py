import mysql.connector
from mysql.connector import Error

db_logger_connection = mysql.connector.connect(
    host="127.0.0.1",
    user="root", 
    password="testing",
    database="distributed_systems_logger",
    auth_plugin='mysql_native_password'
)

def init_logger():
    cursor = db_logger_connection.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS logs (
        Serial_No INT AUTO_INCREMENT PRIMARY KEY,
        op_type VARCHAR(255),
        Shard_id INT,
        Stud_id_low INT,
        Stud_id_high INT  )
    """
    try:
        cursor.execute(create_table_query)
        print("WAL logger initialized successfully.")
    except Error as e:
        print(f"The error '{e}' occurred.")
    finally:
        cursor.close()


def write_log_entry(op_type, shard_id, stud_id_low, stud_id_high):
    cursor = db_logger_connection.cursor()
    insert_query = """
    INSERT INTO logs (op_type, Shard_id, Stud_id_low, Stud_id_high) 
    VALUES (%s, %s, %s, %s)
    """
    values = (op_type, shard_id, stud_id_low, stud_id_high)
    try:
        cursor.execute(insert_query, values)
        print("WAL log written successfully.")
    except Error as e:
        print(f"The error '{e}' occurred.")
    finally:
        cursor.close()


def commit_logs():
    try:
        db_logger_connection.commit()
        print("WAL log committed successfully.")
    except Error as e:
        print(f"The error '{e}' occurred.")