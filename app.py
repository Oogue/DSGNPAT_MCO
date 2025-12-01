from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import mysql.connector
from datetime import datetime

import uuid
from datetime import datetime
import json
from dotenv import load_dotenv
import os
from log_manager import DistributedLogManager
from db_helpers import get_db_connection, DB_CONFIG    
import time
import threading
import traceback

from log_manager import DistributedLogManager
from db_helpers import get_db_connection, DB_CONFIG    

# --- GLOBAL CONCURRENCY SETTINGS ---
GLOBAL_SETTINGS = {
    'isolation_level': 'READ COMMITTED', # Default
    'auto_commit': True,                 # True = Recovery Logic; False = Concurrency Simulation
    'auto_commit_log': True
}

# Stores active connection objects for Manual Mode
# Structure: {txn_id: {'type': 'INSERT', 'status': 'PENDING', 'connections': { 'nodeX': conn_obj }}}
ACTIVE_TXN_CONNECTIONS = {}

SIMULATE_CRASH_MODE = False

load_dotenv()
try:
    LOCAL_NODE_KEY = os.environ.get('LOCAL_NODE_KEY', 'node1') 
    LOCAL_NODE_ID = int(LOCAL_NODE_KEY.replace('node', ''))
    print(f"Local Node Key: {LOCAL_NODE_KEY}, ID: {LOCAL_NODE_ID}")
except Exception as e:
    print(f"Error determining local node from environment: {e}")
    LOCAL_NODE_KEY = 'node1'
    LOCAL_NODE_ID = 1

# Initialize Log Manager
try:
    LOCAL_DB_CONN = mysql.connector.connect(**DB_CONFIG[LOCAL_NODE_KEY])
    LOG_MANAGER = DistributedLogManager(LOCAL_NODE_ID, LOCAL_DB_CONN)
    print(f"Log Manager initialized for {LOCAL_NODE_KEY}. Recovery startup complete.")
except Exception as e:
    print(f"Could not initialize Log Manager or connect to {LOCAL_NODE_KEY}: {e}")
    LOG_MANAGER = None

app = Flask(__name__)
CORS(app)

# --- HELPER: Execute Query with Concurrency Support ---
def execute_query(node_key, query, params=None, commit_immediately=None):
    # Determine mode: Manual Override > Global Setting
    auto_commit_enabled = commit_immediately if commit_immediately is not None else GLOBAL_SETTINGS['auto_commit']
    
    # Connect with specific Isolation Level
    conn = get_db_connection(
        node_key, 
        isolation_level=GLOBAL_SETTINGS['isolation_level'], 
        autocommit_conn=auto_commit_enabled
    )
    
    if not conn:
        return {"success": False, "error": f"Connection to {node_key} failed"}
    
    try:
        cursor = conn.cursor()
        cursor.execute(query, params or ())
        rows_affected = cursor.rowcount
        cursor.close()

        if auto_commit_enabled:
            # Normal Mode: Commit and Close
            conn.commit()
            conn.close()
            return {"success": True, "rows_affected": rows_affected}
        else:
            # Manual Mode: Return the open connection (Holding the Lock)
            return {"success": True, "rows_affected": rows_affected, "conn_obj": conn}
            
    except Exception as e:
        if conn:
            try: conn.rollback(); conn.close()
            except: pass
        return {"success": False, "error": str(e), "rows_affected": 0}

def get_row_count(node_key):
    # Force autocommit for reads to avoid locking table
    conn = get_db_connection(node_key, autocommit_conn=True)
    if not conn: return 0
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM movies")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    except: return 0

def get_last_update(node_key):
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# --- RECOVERY LOGIC ---
def _execute_recovery_cycle():
    conn = get_db_connection(LOCAL_NODE_KEY)
    if not conn: return {"success": False, "error": "Local DB Error"}

    try:
        temp_log_manager = DistributedLogManager(LOCAL_NODE_ID, conn)
        failed_txns = temp_log_manager.get_failed_replications()
        if not failed_txns: return {"success": True, "count": 0}
            
        recovery_logs = []
        recovered_count = 0
        
        for txn in failed_txns:
            txn_id = txn['transaction_id']
            target_node = txn['replication_target']
            try: payload = json.loads(txn['new_value'])
            except: payload = {}
            op_type = txn['operation_type']
            
            # Target Logic
            if not target_node or str(target_node) == '0':
                region = payload.get('region')
                primary = 'node2' if region in ['US', 'JP'] else 'node3'
                if LOCAL_NODE_KEY == primary: target_node = 'node1'
                elif LOCAL_NODE_KEY == 'node1': target_node = primary
                else: target_node = 'node1' 
            
            target_node = str(target_node)
            if target_node.isdigit(): target_node = f"node{target_node}"

            recovery_logs.append(f"Recovering {txn_id} -> {target_node}...")
            
            res = {'success': False}
            if op_type == 'INSERT':
                query = "INSERT INTO movies (titleId, ordering, title, region, language, types, attributes, isOriginalTitle) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE title=title"
                params = (payload.get('titleId'), payload.get('ordering'), payload.get('title'), payload.get('region'), payload.get('language'), payload.get('types'), payload.get('attributes'), payload.get('isOriginalTitle'))
                res = execute_query(target_node, query, params)
            elif op_type == 'UPDATE':
                query = "UPDATE movies SET title = %s, ordering = %s WHERE titleId = %s"
                params = (payload.get('title'), payload.get('ordering'), payload.get('titleId'))
                res = execute_query(target_node, query, params)
            elif op_type == 'DELETE':
                query = "DELETE FROM movies WHERE titleId = %s"
                params = (payload.get('titleId'),)
                res = execute_query(target_node, query, params)
                
            if res['success']:
                temp_log_manager.update_replication_status(txn_id, 'REPLICATION_SUCCESS')
                recovery_logs.append("Success.")
                recovered_count += 1
            else:
                recovery_logs.append(f"Failed: {res.get('error')}")
        
        return {"success": True, "count": recovered_count, "details": recovery_logs}
    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        if conn: conn.close()

def start_background_recovery():
    def task():
        print("Background Recovery Thread Started")
        while True:
            time.sleep(15)
            try:
                _execute_recovery_cycle()
            except: pass
    thread = threading.Thread(target=task, daemon=True)
    thread.start()

# --- ROUTES ---

@app.route('/')
def index(): 
    return render_template('index.html', current_node=LOCAL_NODE_KEY, current_node_id=LOCAL_NODE_ID)

@app.route('/status', methods=['GET'])
def node_status():
    status_report = {}
    for key in DB_CONFIG:
        # Force autocommit for status check to avoid hanging on locks
        conn = get_db_connection(key, autocommit_conn=True)
        if conn:
            status_report[key] = {"status": "ONLINE", "rows": get_row_count(key), "lastUpdate": get_last_update(key)}
            conn.close()
        else:
            status_report[key] = {"status": "OFFLINE", "rows": 0, "lastUpdate": "N/A"}
    
    # Send current settings to frontend
    status_report['current_settings'] = GLOBAL_SETTINGS
    return jsonify(status_report)

@app.route('/movies', methods=['GET'])
def get_movies():
    offset = int(request.args.get('offset', 0))
    limit = int(request.args.get('limit', 100))
    title_id = request.args.get('titleId', '')
    title = request.args.get('title', '')
    region = request.args.get('region', '')
    requested_node = request.args.get('node', 'node1')
    if requested_node not in DB_CONFIG: requested_node = 'node1'

    where_clause = " WHERE 1=1" 
    params = []
    if title_id: where_clause += " AND titleId LIKE %s"; params.append(f"%{title_id}%")
    if title: where_clause += " AND title LIKE %s"; params.append(f"%{title}%")
    if region: where_clause += " AND region LIKE %s"; params.append(f"%{region}%")

    # IMPORTANT: Reader uses the Global Isolation Level
    # If AutoCommit is OFF (Simulation Mode), we DO NOT fallback to Central,
    # because we want to see the lock/dirty-read on the specific node being tested.
    allow_fallback = GLOBAL_SETTINGS['auto_commit'] 
    
    target_node = requested_node
    # Connect with specific isolation level
    conn = get_db_connection(target_node, isolation_level=GLOBAL_SETTINGS['isolation_level'], autocommit_conn=True)
    
    rows = []
    total_count = 0
    source = target_node

    if conn:
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute(f"SELECT COUNT(*) as total FROM movies {where_clause}", params)
            total_count = cursor.fetchone()['total']
            
            if total_count > 0 or (not title_id and not title and not region):
                cursor.execute(f"SELECT * FROM movies {where_clause} LIMIT %s OFFSET %s", params + [limit, offset])
                rows = cursor.fetchall()
            elif allow_fallback and requested_node != 'node1':
                # Fallback Logic (Only if not simulating concurrency)
                conn.close()
                conn = get_db_connection('node1', isolation_level=GLOBAL_SETTINGS['isolation_level'], autocommit_conn=True)
                if conn:
                    cursor = conn.cursor(dictionary=True)
                    cursor.execute(f"SELECT COUNT(*) as total FROM movies {where_clause}", params)
                    total_count = cursor.fetchone()['total']
                    cursor.execute(f"SELECT * FROM movies {where_clause} LIMIT %s OFFSET %s", params + [limit, offset])
                    rows = cursor.fetchall()
                    source = 'node1 (Fallback)'
        except Exception as e:
            print(f"Read Error: {e}")
        finally:
            if conn and conn.is_connected(): conn.close()

    return jsonify({"data": rows, "total": total_count, "source_node": source})

# --- INSERT (Combined Logic) ---
@app.route('/insert', methods=['POST'])
def insert_movie():
    try:
        data = request.json
        txn_id = str(uuid.uuid4())
        region = data.get('region')
        primary_target_node = 'node2' if region in ['US', 'JP'] else 'node3'
        
        params = (data.get('titleId'), data.get('ordering'), data.get('title'), region, data.get('language'), data.get('types'), data.get('attributes'), data.get('isOriginalTitle'))
        query = "INSERT INTO movies (titleId, ordering, title, region, language, types, attributes, isOriginalTitle) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

        # === CONCURRENCY SIMULATION MODE (Auto Commit OFF) ===
        if not GLOBAL_SETTINGS['auto_commit']:
            # Execute on Primary ONLY (to create the lock) and HOLD.
            print(f"Manual Mode: Locking {primary_target_node}")
            res = execute_query(primary_target_node, query, params, commit_immediately=False)
            
            if res['success']:
                ACTIVE_TXN_CONNECTIONS[txn_id] = {
                    'type': 'INSERT', 
                    'status': 'PENDING_MANUAL',
                    'connections': { primary_target_node: res['conn_obj'] }
                }
                return jsonify({"status": "MANUAL_PENDING", "txn_id": txn_id, "logs": [f"Paused INSERT on {primary_target_node}. Row locked."]})
            else:
                return jsonify({"status": "FAILED", "error": res.get('error')})

        # === RECOVERY V3 MODE (Auto Commit ON) ===
        replication_target_node = 'node1' if primary_target_node != 'node1' else None
        logs = []
        
        res_primary = execute_query(primary_target_node, query, params)
        LOG_MANAGER.log_local_commit(txn_id, 'INSERT', data.get('titleId'), data)
        
        if SIMULATE_CRASH_MODE: time.sleep(10)
        
        primary_success = res_primary['success']
        if not primary_success:
            LOG_MANAGER.log_replication_attempt(txn_id, primary_target_node)
            LOG_MANAGER.update_replication_status(txn_id, 'REPLICATION_FAILED')
            logs.append(f"Primary {primary_target_node} Failed. Queued.")

        if replication_target_node:
            res_replica = execute_query(replication_target_node, query, params)
            if res_replica['success'] and primary_success:
                LOG_MANAGER.log_replication_attempt(txn_id, replication_target_node)
                LOG_MANAGER.update_replication_status(txn_id, 'REPLICATION_SUCCESS')
            elif not res_replica['success']:
                LOG_MANAGER.log_replication_attempt(txn_id, replication_target_node)
                LOG_MANAGER.update_replication_status(txn_id, 'REPLICATION_FAILED')
                logs.append(f"Replica {replication_target_node} Failed. Queued.")

        return jsonify({"status": "COMPLETED", "txn_id": txn_id, "logs": logs})

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# --- UPDATE (Combined Logic) ---
@app.route('/update', methods=['POST'])
def update_movie():
    try:
        data = request.json
        txn_id = str(uuid.uuid4())
        title_id = data.get('titleId')
        region = data.get('region') # Assumed passed from frontend
        
        # Determine Primary based on Region (if known) or fallback
        if not region: 
            # Simple routing guess if region missing
            res_n2 = execute_query('node2', "SELECT region FROM movies WHERE titleId=%s", (title_id,), commit_immediately=True)
            primary_target_node = 'node2' if res_n2['rows_affected'] > 0 else 'node3'
        else:
            primary_target_node = 'node2' if region in ['US', 'JP'] else 'node3'

        params = (data.get('title'), data.get('ordering'), title_id)
        query = "UPDATE movies SET title = %s, ordering = %s WHERE titleId = %s"

        # === CONCURRENCY SIMULATION MODE ===
        if not GLOBAL_SETTINGS['auto_commit']:
            res = execute_query(primary_target_node, query, params, commit_immediately=False)
            if res['success']:
                ACTIVE_TXN_CONNECTIONS[txn_id] = {
                    'type': 'UPDATE', 
                    'status': 'PENDING_MANUAL',
                    'connections': { primary_target_node: res['conn_obj'] }
                }
                return jsonify({"status": "MANUAL_PENDING", "txn_id": txn_id, "logs": [f"Paused UPDATE on {primary_target_node}. Row locked."]})
            return jsonify({"status": "FAILED"})

        # === RECOVERY V3 MODE ===
        replication_target_node = 'node1' if primary_target_node != 'node1' else None
        res_primary = execute_query(primary_target_node, query, params)
        
        # Log Logic (Simplified)
        LOG_MANAGER.log_local_commit(txn_id, 'UPDATE', title_id, data)
        if SIMULATE_CRASH_MODE: time.sleep(10)
        
        if not res_primary['success']:
            LOG_MANAGER.log_replication_attempt(txn_id, primary_target_node)
            LOG_MANAGER.update_replication_status(txn_id, 'REPLICATION_FAILED')

        if replication_target_node:
            execute_query(replication_target_node, query, params) # Simple execute for replica

        return jsonify({"status": "COMPLETED", "txn_id": txn_id})

    except Exception as e:
        traceback.print_exc()
        return jsonify({
            "status": "CRASH",
            "error": "Internal Server Error trapped",
            "details": str(e),
            "logs": ["CRITICAL ERROR: " + str(e)]
        }), 500

# --- DELETE (Combined Logic) ---
@app.route('/delete', methods=['POST'])
def delete_movie():
    try:
        data = request.json
        txn_id = str(uuid.uuid4())
        title_id = data.get('titleId')
        region = data.get('region') # Needed for routing
        
        if not region:
            # Query Node 1 (Central) to find where this movie actually lives
            # We use dirty read (isolation level) or just a quick lookup
            check_res = execute_query('node1', "SELECT region FROM movies WHERE titleId = %s", (title_id,))
            
            # You'll need to adjust execute_query in the new version to return data, 
            # or use a helper like get_db_connection directly here.
            # Assuming you can get the row:
            conn = get_db_connection('node1')
            cur = conn.cursor()
            cur.execute("SELECT region FROM movies WHERE titleId = %s", (title_id,))
            row = cur.fetchone()
            if row:
                region = row[0]
            conn.close()

        primary_target_node = 'node2' if region in ['US', 'JP'] else 'node3'

        # === CONCURRENCY SIMULATION MODE ===
        if not GLOBAL_SETTINGS['auto_commit']:
            res = execute_query(primary_target_node, query, params, commit_immediately=False)
            if res['success']:
                ACTIVE_TXN_CONNECTIONS[txn_id] = {
                    'type': 'DELETE', 
                    'status': 'PENDING_MANUAL',
                    'connections': { primary_target_node: res['conn_obj'] }
                }
                return jsonify({"status": "MANUAL_PENDING", "txn_id": txn_id, "logs": [f"Paused DELETE on {primary_target_node}. Row locked."]})
            return jsonify({"status": "FAILED"})

        # === RECOVERY V3 MODE ===
        replication_target_node = 'node1' if primary_target_node != 'node1' else None
        res_primary = execute_query(primary_target_node, query, params)
        LOG_MANAGER.log_local_commit(txn_id, 'DELETE', title_id, {'titleId': title_id, 'region': region})
        
        if SIMULATE_CRASH_MODE: time.sleep(10)
        
        if not res_primary['success']:
            LOG_MANAGER.log_replication_attempt(txn_id, primary_target_node)
            LOG_MANAGER.update_replication_status(txn_id, 'REPLICATION_FAILED')

        if replication_target_node:
            execute_query(replication_target_node, query, params)

        return jsonify({"status": "COMPLETED", "txn_id": txn_id})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# --- SETTINGS & MANUAL RESOLUTION ---

@app.route('/settings', methods=['POST'])
def update_settings():
    data = request.json
    if 'isolationLevel' in data:
        GLOBAL_SETTINGS['isolation_level'] = data['isolationLevel']
    if 'autoCommit' in data:
        # Convert string "true"/"false" to boolean if needed
        val = data['autoCommit']
        if isinstance(val, str): val = val.lower() == 'true'
        GLOBAL_SETTINGS['auto_commit'] = val
    
    return jsonify({"status": "Updated", "settings": GLOBAL_SETTINGS})

@app.route('/active-transactions', methods=['GET'])
def get_active_transactions():
    """List open manual transactions for the modal."""
    report = {}
    for txn_id, info in ACTIVE_TXN_CONNECTIONS.items():
        node_names = list(info['connections'].keys())
        report[txn_id] = {
            'type': info['type'],
            'status': info['status'],
            'node': ", ".join(node_names)
        }
    return jsonify(report)

@app.route('/resolve-transaction', methods=['POST'])
def resolve_transaction():
    data = request.json
    txn_id = data.get('txnId')
    action = data.get('action') # COMMIT or ROLLBACK
    
    if txn_id not in ACTIVE_TXN_CONNECTIONS:
        return jsonify({"message": "Transaction not found", "success": False}), 404
        
    txn_info = ACTIVE_TXN_CONNECTIONS[txn_id]
    logs = []
    
    for node, conn in txn_info['connections'].items():
        try:
            if action == 'COMMIT':
                conn.commit()
                logs.append(f"{node}: Committed.")
            else:
                conn.rollback()
                logs.append(f"{node}: Rolled Back.")
            conn.close()
        except Exception as e:
            logs.append(f"{node} Error: {e}")
            
    del ACTIVE_TXN_CONNECTIONS[txn_id]
    return jsonify({"message": "Resolved", "logs": logs})

@app.route('/toggle-crash-mode', methods=['POST'])
def toggle_crash_mode():
    global SIMULATE_CRASH_MODE
    SIMULATE_CRASH_MODE = not SIMULATE_CRASH_MODE
    return jsonify({"status": "ENABLED" if SIMULATE_CRASH_MODE else "DISABLED"})

if __name__ == '__main__':
    start_background_recovery()
    app.run(host='0.0.0.0', port=80)