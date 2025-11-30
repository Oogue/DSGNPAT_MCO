from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import mysql.connector
from datetime import datetime
import uuid
import json
from dotenv import load_dotenv
import os

# Assuming log_manager and db_helpers are available and contain necessary classes/funcs
from log_manager import DistributedLogManager
from db_helpers import get_db_connection, DB_CONFIG # Relying on DB_CONFIG from db_helpers

# Global Configuration Dictionary
GLOBAL_SETTINGS = {
    'isolation_level': 'READ COMMITTED', # Default Isolation Level
    'auto_commit': True,                 # True: Atomic DDM & Logging; False: Manual 2PC Mode
    'auto_commit_log': True              # Autocommit logs table operations
}

# Stores active connection objects for transactions waiting for manual commit (when auto_commit=False).
# Structure: {txn_id: {'type': 'INSERT', 'status': 'PENDING', 'node1': conn_obj, ...}}
ACTIVE_TXN_CONNECTIONS = {}

load_dotenv()
try:
    LOCAL_NODE_KEY = os.environ.get('LOCAL_NODE_KEY', 'node1') 
    LOCAL_NODE_ID = int(LOCAL_NODE_KEY.replace('node', ''))
    print(f"Local Node Key: {LOCAL_NODE_KEY}, ID: {LOCAL_NODE_ID}")
except Exception as e:
    print(f"Error determining local node from environment: {e}")
    LOCAL_NODE_KEY = 'node1'
    LOCAL_NODE_ID = 1

# Initialize Log Manager for Local Node
try:
    # Note: Using the provided DB_CONFIG structure for connection
    LOCAL_DB_CONN = mysql.connector.connect(**DB_CONFIG[LOCAL_NODE_KEY])
    # Pass the global log autocommit setting to the Log Manager
    LOG_MANAGER = DistributedLogManager(
        LOCAL_NODE_ID, 
        LOCAL_DB_CONN,
        auto_commit_log=GLOBAL_SETTINGS['auto_commit_log'] # Pass the flag here
    )
    print(f"Log Manager initialized for {LOCAL_NODE_KEY}. Recovery startup complete.")
except Exception as e:
    print(f"Could not initialize Log Manager or connect to {LOCAL_NODE_KEY}: {e}")
    LOG_MANAGER = None

# Initialize the Flask application
app = Flask(__name__)
CORS(app)

# Database configuration
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

# --- HELPER FUNCTION: Execute Query (Modified for Flexibility) ---
def execute_query(node_key, query, params=None, commit_immediately=None):
    # Determine autocommit preference for this specific connection
    # commit_immediately can override the global setting for multi-node atomicity
    auto_commit_enabled = commit_immediately if commit_immediately is not None else GLOBAL_SETTINGS['auto_commit']
    
    conn = get_db_connection(
        node_key, 
        isolation_level=GLOBAL_SETTINGS['isolation_level'], 
        autocommit_conn=auto_commit_enabled
    )
    
    if not conn:
        return {"success": False, "error": f"Connection to {node_key} failed", "rows_affected": 0}
    
    try:
        cursor = conn.cursor()
        cursor.execute(query, params or ())
        rows_affected = cursor.rowcount
        cursor.close()

        if auto_commit_enabled:
            # Standard auto-commit mode: commit and close immediately
            conn.commit()
            conn.close()
            return {"success": True, "rows_affected": rows_affected}
        else:
            # MANUAL MODE: Do not commit, do not close.
            # Return the connection object for manual resolution (2PC or Atomic DDM)
            return {"success": True, "rows_affected": rows_affected, "conn_obj": conn}
            
    except Exception as e:
        if conn:
            # Rollback the uncommitted transaction before closing
            conn.rollback() 
            conn.close()
        return {"success": False, "error": str(e), "rows_affected": 0}

# --- ATOMIC DDM / LOGGING HELPER ---
def perform_atomic_distributed_op(op_type, txn_id, node_ops):
    """
    Handles the multi-node operation atomically (commit/rollback) 
    and integrates logging for the central node (Node 1).
    
    op_type: INSERT/UPDATE/DELETE
    txn_id: unique transaction ID
    node_ops: list of {'node': str, 'query': str, 'params': tuple, 'is_central': bool}
    """
    
    logs = []
    connections = {}
    
    all_successful = True
    
    try:
        # Phase 1: Execute all queries in manual transaction mode
        for op in node_ops:
            node_key = op['node']
            query = op['query']
            params = op['params']
            
            # Execute without committing
            res = execute_query(node_key, query, params, commit_immediately=False)
            logs.append(f"{node_key}: Executed query. Rows Affected: {res['rows_affected']}. Success: {res['success']}")
            
            if not res['success'] or res['rows_affected'] == 0:
                # If a query fails or affects 0 rows (for U/D/D), mark transaction for rollback
                # Allow 0 rows affected only for updates/deletes where the data might not be on that fragment
                if op_type in ['INSERT'] or res['rows_affected'] > 0:
                    all_successful = all_successful and res['success']
                
            if res['success']:
                connections[node_key] = res['conn_obj']
            else:
                # If execution failed (e.g., SQL error), no connection object is returned, mark failure
                all_successful = False
                
        # Phase 2: Logging and Final Commit/Rollback
        if all_successful:
            # --- Distributed Commit/Logging ---
            logs.append("Phase 2: All operations succeeded. Initiating COMMIT and LOGGING.")
            
            central_node_op = next(op for op in node_ops if op['is_central'])
            record_key = central_node_op['params'][-1] # Assuming titleId is the last param
            
            # 1. Log Local Commit (Point of No Return)
            if LOG_MANAGER:
                LOG_MANAGER.log_local_commit(txn_id, op_type, record_key, json.dumps(dict(zip(['title', 'ordering'], central_node_op['params']))))
                logs.append("Log Manager: LOCAL_COMMIT recorded.")
            
            # 2. Final Commit on all nodes
            for node_key, conn in connections.items():
                if node_key != 'node1':
                    # Log Replication Attempt before committing
                    if LOG_MANAGER:
                        LOG_MANAGER.log_replication_attempt(txn_id, int(node_key.replace('node', '')))
                        
                conn.commit()
                
                if node_key != 'node1' and LOG_MANAGER:
                    # Update Replication Status after successful commit
                    LOG_MANAGER.update_replication_status(txn_id, int(node_key.replace('node', '')), True)
                    
                logs.append(f"{node_key}: COMMIT successful.")
                
        else:
            # --- Distributed Rollback ---
            logs.append("Phase 2: One or more operations failed. Initiating ROLLBACK.")
            for node_key, conn in connections.items():
                conn.rollback()
                logs.append(f"{node_key}: ROLLBACK executed.")
                
    except Exception as e:
        logs.append(f"CRITICAL ERROR during atomic operation: {e}. Initiating ROLLBACK on remaining connections.")
        for node_key, conn in connections.items():
             try:
                 conn.rollback()
             except:
                 logs.append(f"{node_key}: Rollback failed.")
        all_successful = False
        
    finally:
        # Phase 3: Close all connections
        for node_key, conn in connections.items():
            try:
                conn.close()
            except:
                pass

    return all_successful, logs

def get_row_count(node_key):
    """Get the total number of rows in a node"""
    # Force autocommit for simple read
    conn = get_db_connection(node_key, autocommit_conn=True) 
    if not conn:
        return 0
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM movies")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return count
    except Exception as e:
        print(f"Error counting rows in {node_key}: {e}")
        return 0

def get_last_update(node_key):
    # TODO: Implement actual last update tracking
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
# Frontend / Homepage

@app.route('/')
def index(): 
    return render_template('index.html')

# ROUTE: Status with detailed information
@app.route('/status', methods=['GET'])
def node_status():
    status_report = {}
    for key in DB_CONFIG:
        # Force autocommit = true for connection status check
        conn = get_db_connection(key, autocommit_conn=True)
        if conn:
            row_count = get_row_count(key)
            last_update = get_last_update(key)
            status_report[key] = {
                "status": "ONLINE",
                "rows": row_count,
                "lastUpdate": last_update
            }
            conn.close()
        else:
            status_report[key] = {
                "status": "OFFLINE",
                "rows": 0,
                "lastUpdate": "N/A"
            }
    
    # Add local node information
    status_report['local_node_id'] = LOCAL_NODE_ID
    status_report['local_node_key'] = LOCAL_NODE_KEY
    status_report['active_transactions'] = list(ACTIVE_TXN_CONNECTIONS.keys())
    status_report['current_settings'] = GLOBAL_SETTINGS
    
    return jsonify(status_report)

@app.route('/movies', methods=['GET'])
def get_movies():
    # Get query parameters
    # always auto-commit read operation
    offset = int(request.args.get('offset', 0))
    limit = int(request.args.get('limit', 100))
    # Filter parameters
    title_id = request.args.get('titleId', '')
    title = request.args.get('title', '')
    region = request.args.get('region', '')
    # Node selection
    requested_node = request.args.get('node', 'node1')
    if requested_node not in DB_CONFIG:
        requested_node = 'node1'

    # Build Query
    where_clause = " WHERE 1=1" 
    params = []
    if title_id:
        where_clause += " AND titleId LIKE %s"
        params.append(f"%{title_id}%")
    if title:
        where_clause += " AND title LIKE %s"
        params.append(f"%{title}%")
    if region:
        where_clause += " AND region LIKE %s"
        params.append(f"%{region}%")

    target_node = requested_node
    conn = get_db_connection(target_node, autocommit_conn=True)
    
    rows = []
    total_count = 0
    source = target_node

    if conn:
        cursor = conn.cursor(dictionary=True)
        try:
            # Count
            cursor.execute(f"SELECT COUNT(*) as total FROM movies {where_clause}", params)
            total_count = cursor.fetchone()['total']
            
            if total_count > 0 or (not title_id and not title and not region):
                cursor.execute(f"SELECT * FROM movies {where_clause} LIMIT %s OFFSET %s", params + [limit, offset])
                rows = cursor.fetchall()
            else:
                # Fallback to Central (Node 1) if a fragment returns 0 results on a specific search
                if requested_node != 'node1':
                    conn.close()
                    conn_central = get_db_connection('node1', autocommit_conn=True)
                    if conn_central:
                        cursor_central = conn_central.cursor(dictionary=True)
                        cursor_central.execute(f"SELECT COUNT(*) as total FROM movies {where_clause}", params)
                        total_count = cursor_central.fetchone()['total']
                        cursor_central.execute(f"SELECT * FROM movies {where_clause} LIMIT %s OFFSET %s", params + [limit, offset])
                        rows = cursor_central.fetchall()
                        conn_central.close()
                        source = 'node1 (Fallback)'
        except Exception as e:
            print(f"Read Error: {e}")
            total_count = 0
            rows = []
        finally:
            if conn.is_connected():
                conn.close()

    return jsonify({
        "data": rows,
        "total": total_count,
        "source_node": source
    })

# ROUTE: Insert
@app.route('/insert', methods=['POST'])
def insert_movie():
    data = request.json
    txn_id = str(uuid.uuid4())
    auto_commit = GLOBAL_SETTINGS['auto_commit']
    
    params = (
        data.get('titleId'), data.get('ordering'), data.get('title'),
        data.get('region'), data.get('language'), data.get('types'),
        data.get('attributes'), data.get('isOriginalTitle')
    )
    query = """
        INSERT INTO movies 
        (titleId, ordering, title, region, language, types, attributes, isOriginalTitle) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    target_region = data.get('region')
    correct_fragment = 'node3' 
    if target_region in ['US', 'JP']: 
        correct_fragment = 'node2'
    
    # Define operations for atomic/logged execution
    node_ops = [
        {'node': 'node1', 'query': query, 'params': params, 'is_central': True},
        {'node': correct_fragment, 'query': query, 'params': params, 'is_central': False}
    ]
    
    if auto_commit:
        success, logs = perform_atomic_distributed_op('INSERT', txn_id, node_ops)
        message = "Insert Committed" if success else "Insert Failed and Rolled Back"
        return jsonify({"message": message, "txn_id": txn_id, "logs": logs, "success": success})
    else:
        # Manual 2PC Logic (The existing logic)
        ACTIVE_TXN_CONNECTIONS[txn_id] = {'type': 'INSERT', 'status': 'PENDING'}
        logs = []
        
        for op in node_ops:
            node_key = op['node']
            res = execute_query(node_key, op['query'], op['params'], commit_immediately=False)
            logs.append(f"{node_key}: Executed (Ready for Manual Commit)")
            
            if res['success']:
                ACTIVE_TXN_CONNECTIONS[txn_id][node_key] = res['conn_obj']
            
        return jsonify({"message": "Insert Executed. Waiting for Manual Resolve.", "txn_id": txn_id, "logs": logs, "success": True})

# ROUTE: Update
@app.route('/update', methods=['POST'])
def update_movie():
    data = request.json
    txn_id = str(uuid.uuid4())
    auto_commit = GLOBAL_SETTINGS['auto_commit']
    
    title_id = data.get('titleId')
    new_title = data.get('title')
    new_ordering = data.get('ordering')
    
    query = "UPDATE movies SET title = %s, ordering = %s WHERE titleId = %s"
    params = (new_title, new_ordering, title_id)
    
    # Strategy for Update: Try Node 2, if 0 rows affected, try Node 3
    
    # 1. Check Node 2 first
    res_node2 = execute_query('node2', "SELECT titleId FROM movies WHERE titleId = %s", (title_id,), commit_immediately=True)
    correct_fragment = 'node2' if res_node2['rows_affected'] > 0 else 'node3'
    
    node_ops = [
        {'node': 'node1', 'query': query, 'params': params, 'is_central': True},
        {'node': correct_fragment, 'query': query, 'params': params, 'is_central': False}
    ]
    
    if auto_commit:
        success, logs = perform_atomic_distributed_op('UPDATE', txn_id, node_ops)
        message = "Update Committed" if success else "Update Failed and Rolled Back"
        return jsonify({"message": message, "txn_id": txn_id, "logs": logs, "success": success})
    else:
        # Manual 2PC Logic
        ACTIVE_TXN_CONNECTIONS[txn_id] = {'type': 'UPDATE', 'status': 'PENDING'}
        logs = []
        
        for op in node_ops:
            node_key = op['node']
            res = execute_query(node_key, op['query'], op['params'], commit_immediately=False)
            logs.append(f"{node_key}: Executed (Ready for Manual Commit)")
            
            if res['success']:
                ACTIVE_TXN_CONNECTIONS[txn_id][node_key] = res['conn_obj']
        
        return jsonify({"message": "Update Executed. Waiting for Manual Resolve.", "txn_id": txn_id, "logs": logs, "success": True})

# ROUTE: Delete
@app.route('/delete', methods=['POST'])
def delete_movie():
    data = request.json
    txn_id = str(uuid.uuid4())
    auto_commit = GLOBAL_SETTINGS['auto_commit']
    
    title_id = data.get('titleId')
    
    query = "DELETE FROM movies WHERE titleId = %s"
    params = (title_id,)
    
    # Strategy for Delete: Try Node 2, if 0 rows affected, try Node 3
    
    # 1. Check Node 2 first
    res_node2 = execute_query('node2', "SELECT titleId FROM movies WHERE titleId = %s", (title_id,), commit_immediately=True)
    correct_fragment = 'node2' if res_node2['rows_affected'] > 0 else 'node3'
    
    node_ops = [
        {'node': 'node1', 'query': query, 'params': params, 'is_central': True},
        {'node': correct_fragment, 'query': query, 'params': params, 'is_central': False}
    ]
    
    if auto_commit:
        success, logs = perform_atomic_distributed_op('DELETE', txn_id, node_ops)
        message = "Delete Committed" if success else "Delete Failed and Rolled Back"
        return jsonify({"message": message, "txn_id": txn_id, "logs": logs, "success": success})
    else:
        # Manual 2PC Logic
        ACTIVE_TXN_CONNECTIONS[txn_id] = {'type': 'DELETE', 'status': 'PENDING'}
        logs = []
        
        for op in node_ops:
            node_key = op['node']
            res = execute_query(node_key, op['query'], op['params'], commit_immediately=False)
            logs.append(f"{node_key}: Executed (Ready for Manual Commit)")
            
            # Special logic for delete: only store if rows were affected, otherwise close connection immediately
            if res['success'] and res.get('rows_affected', 0) > 0:
                ACTIVE_TXN_CONNECTIONS[txn_id][node_key] = res['conn_obj']
            elif res['success'] and res.get('rows_affected', 0) == 0 and res.get('conn_obj'):
                res['conn_obj'].close()
                logs.append(f"{node_key}: No rows affected, connection closed.")
        
        return jsonify({"message": "Delete Executed. Waiting for Manual Resolve.", "txn_id": txn_id, "logs": logs, "success": True})


# ROUTE: Get active transactions (Remains for Manual 2PC mode)
@app.route('/active-transactions', methods=['GET'])
def get_active_transactions():
    """Returns a list of currently active transaction IDs."""
    report = {}
    for txn_id, txn_data in ACTIVE_TXN_CONNECTIONS.items():
        conn_count = len([k for k in txn_data.keys() if k not in ['type', 'status']])
        
        report[txn_id] = {
            'type': txn_data.get('type', 'Unknown'),
            'status': txn_data.get('status', 'PENDING'),
            'connection_count': conn_count
        }

    return jsonify(report)

# ROUTE: Manually Commit or Rollback a Transaction (Remains for Manual 2PC mode)
@app.route('/resolve-transaction', methods=['POST'])
def resolve_transaction():
    global ACTIVE_TXN_CONNECTIONS
    data = request.json
    txn_id = data.get('txnId')
    action = data.get('action') # 'COMMIT' or 'ROLLBACK'

    if txn_id not in ACTIVE_TXN_CONNECTIONS:
        return jsonify({"message": f"Transaction ID {txn_id} not found or already resolved.", "success": False}), 404

    txn_data = ACTIVE_TXN_CONNECTIONS[txn_id]
    logs = [f"Attempting {action} for Transaction {txn_id} ({txn_data['type']})..."]
    success_count = 0
    failure_count = 0

    # Iterate over all stored connection objects for this transaction
    for node_key, conn in list(txn_data.items()):
        if node_key in ['type', 'status']:
            continue
            
        try:
            if action == 'COMMIT':
                conn.commit()
                logs.append(f"{node_key}: Committed successfully.")
                success_count += 1
            elif action == 'ROLLBACK':
                conn.rollback()
                logs.append(f"{node_key}: Rolled back successfully.")
                success_count += 1
            else:
                logs.append(f"Invalid action specified: {action}. Connection for {node_key} left open.")
                failure_count += 1
                continue
            
            # Always close the connection after the final action
            conn.close()

        except Exception as e:
            logs.append(f"{node_key}: Failed to {action}. Error: {e}")
            try:
                conn.close()
            except:
                pass 
            failure_count += 1

    txn_data['status'] = f"{action}ED"
    del ACTIVE_TXN_CONNECTIONS[txn_id]

    final_message = f"Transaction {txn_id} resolved: {success_count} successful actions, {failure_count} failures."
    
    return jsonify({
        "message": final_message, 
        "logs": logs,
        "success": failure_count == 0
    })

# ROUTE: Simulate Concurrency
@app.route('/simulate-concurrency', methods=['POST'])
def simulate_concurrency():
    """
    TODO: Implement concurrency simulation
    
    This endpoint should:
    1. Create multiple concurrent transactions
    2. Test different isolation levels
    3. Simulate race conditions
    4. Test deadlock scenarios
    5. Monitor transaction conflicts
    6. Return detailed logs of concurrent operations
    
    Example implementation:
    - Spawn multiple threads/processes
    - Execute simultaneous reads/writes
    - Track transaction timing and conflicts
    - Return results showing concurrency behavior
    """
    
    return jsonify({
        "message": "Concurrency simulation not yet implemented",
        "status": "TODO"
    })

# ROUTE: Settings
@app.route('/settings', methods=['POST'])
def update_settings():
    global GLOBAL_SETTINGS
    data = request.json
    logs = []
    
    # --- Update Isolation Level Setting ---
    isolation_level = data.get('isolationLevel')
    if isolation_level and isolation_level in ['READ UNCOMMITTED', 'READ COMMITTED', 'REPEATABLE READ', 'SERIALIZABLE']:
        GLOBAL_SETTINGS['isolation_level'] = isolation_level
        logs.append(f"Set Global Isolation Level to: {isolation_level}")

    # --- Update Transaction Autocommit Setting ---
    auto_commit_str = data.get('autoCommit')
    if auto_commit_str is not None:
        auto_commit = auto_commit_str.lower() == 'true'
        GLOBAL_SETTINGS['auto_commit'] = auto_commit
        logs.append(f"Set Global Transaction Autocommit (DDM Mode) to: {auto_commit}")
        
    # --- Update Log Autocommit Setting ---
    auto_commit_log_str = data.get('autoCommitLog')
    if auto_commit_log_str is not None:
        auto_commit_log = auto_commit_log_str.lower() == 'true'
        GLOBAL_SETTINGS['auto_commit_log'] = auto_commit_log
        
        if LOG_MANAGER:
            LOG_MANAGER.auto_commit_log = auto_commit_log
            logs.append(f"Updated LOG_MANAGER Autocommit to: {auto_commit_log}")
        
    return jsonify({
        "status": "Settings updated successfully", 
        "current_settings": GLOBAL_SETTINGS,
        "logs": logs
    })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)