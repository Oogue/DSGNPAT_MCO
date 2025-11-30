import mysql.connector
# Note: You may need to load_dotenv() and define DB_CONFIG here
DB_CONFIG = {
    'node1': {
        'host': '10.2.14.84', 
        'user': 'admin',
        'password': 'poginiallen',     
        'database': 'mco2_ddb'       
    },
    'node2': {
        'host': '10.2.14.85',   
        'user': 'admin',
        'password': 'poginiallen',
        'database': 'mco2_ddb'
    },
    'node3': {
        'host': '10.2.14.86',   
        'user': 'admin',
        'password': 'poginiallen',
        'database': 'mco2_ddb'
    }
}

def get_db_connection(node_key, isolation_level = None, autocommit_conn = True):
    try:
        config = DB_CONFIG[node_key]
        conn = mysql.connector.connect(**config)

        if isolation_level:
            conn.isolation_level = isolation_level
            
        conn.autocommit = autocommit_conn

        return conn
    except Exception as e:
        print(f"Error connecting to {node_key}: {e}")
        return None