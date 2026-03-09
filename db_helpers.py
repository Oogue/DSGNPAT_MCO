import mysql.connector
# Note: You may need to load_dotenv() and define DB_CONFIG here
DB_CONFIG = {
    'node1': {
        'host': '127.0.0.1', 
        'user': 'root',         
        'password': 'Hulaanmo0!',
        'database': 'mco2_ddb_node1'       
    },
    'node2': {
        'host': '127.0.0.1',   
        'user': 'root',
        'password': 'Hulaanmo0!',
        'database': 'mco2_ddb_node2'
    },
    'node3': {
        'host': '127.0.0.1',   
        'user': 'root',
        'password': 'Hulaanmo0!',
        'database': 'mco2_ddb_node3'
    }
}

def get_db_connection(node_key, isolation_level=None, autocommit_conn=True):
    try:
        config = DB_CONFIG[node_key]
        conn = mysql.connector.connect(**config)

        # Should enforces the lock rules for this session.
        if isolation_level:
            cursor = conn.cursor()
            # Ensure proper syntax (spaces instead of dashes)
            level_sql = isolation_level.replace('-', ' ').replace('_', ' ').upper() 
            cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {level_sql}")
            cursor.close()
            
        conn.autocommit = autocommit_conn

        return conn
    except Exception as e:
        print(f"Error connecting to {node_key}: {e}")
        return None