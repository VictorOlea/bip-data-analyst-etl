from sqlalchemy import create_engine
import time

def connection_postgresql():
    user = 'your user (postgres)'
    password = "your password"
    host = 'your host (localhost)'
    port = 'your port (5432)'
    database = 'your database name'
    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{database}'
    )
    
    conn = None
    
    try:
        conn = engine.connect()
        print(f"Test Connection to PostgreSQl was Successful!!!")
    except Exception as error:
        print(f"Error: {error}")
    finally:
        if conn is not None:
            conn.close()
            print("Connection Close")
    
if __name__ == "__main__":
    
    start_time = time.time()

    connection_postgresql()

    end_time = time.time()
    total_time = end_time - start_time
    print(f"Connection time: {total_time} seconds")
    