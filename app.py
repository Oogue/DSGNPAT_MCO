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
from RecoveryMemento import RecoveryState, RecoveryCaretaker

from log_manager import DistributedLogManager
from db_helpers import get_db_connection, DB_CONFIG    

# --- GLOBAL CONCURRENCY SETTINGS ---
GLOBAL_SETTINGS = {
    'isolation_level': 'READ COMMITTED', # Default
    'auto_commit': True,                 # True = Recovery Logic; False = Concurrency Simulation
    'auto_commit_log': True,
    'simulate_blocking': False
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
    auto_commit_enabled = commit_immediately if commit_immediately is not None else GLOBAL_SETTINGS['auto_commit']
    
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
    if not conn:
        return {"success": False, "error": "Local DB Error"}

    try:
        log_manager = DistributedLogManager(LOCAL_NODE_ID, conn)
        failed_txns = log_manager.get_failed_replications()
        if not failed_txns:
            return {"success": True, "count": 0}

        state = RecoveryState()
        caretaker = RecoveryCaretaker(state)

        for txn in failed_txns:
            # log the attempt first before doing the checkpoint
            state.log_attempt(txn_id, target_node)
            caretaker.checkpoint() # create the snapshot 

            txn_id = txn['transaction_id']
            target_node = txn['replication_target']
            try:
                payload = json.loads(txn['new_value'])
            except:
                payload = {}
            op_type = txn['operation_type']

            # Target resolution (same as original)
            if not target_node or str(target_node) == '0':
                region = payload.get('region')
                primary = 'node2' if region in ['US', 'JP'] else 'node3'
                if LOCAL_NODE_KEY == primary:
                    target_node = 'node1'
                elif LOCAL_NODE_KEY == 'node1':
                    target_node = primary
                else:
                    target_node = 'node1'

            target_node = str(target_node)
            if target_node.isdigit():
                target_node = f"node{target_node}"

            # state.log_attempt(txn_id, target_node)

            # Execute query
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
                state.mark_success(txn_id, log_manager)
            else: # if failure, rollback to revert to the last state
                caretaker.rollback()   
                state.mark_failure(txn_id, res.get('error'))

        return {"success": True, "count": state.recovered_count, "details": state.recovery_logs}
    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            conn.close()



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
    
    # Validate node
    if requested_node not in DB_CONFIG: 
        requested_node = 'node1'

    # Build SQL Filter
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

    allow_fallback = True
    target_node = requested_node
    reader_autocommit = not GLOBAL_SETTINGS.get('simulate_blocking', False)
    
    # 1. Initial connection attempt (to target_node)
    conn = get_db_connection(
        target_node, 
        isolation_level=GLOBAL_SETTINGS['isolation_level'], 
        autocommit_conn=reader_autocommit
    )
    
    rows = []
    total_count = 0
    source = target_node

    # Initialize all potential fallback connections to None
    conn_node1 = None
    conn_for_node2 = None
    conn_for_node3 = None
    
    # Flag to control fallback to aggregation
    trigger_aggregation_fallback = False

    if conn:
        cursor = conn.cursor(dictionary=True)
        try:
            # 1a. Attempt to fetch from the requested node (N1, N2, or N3)
            cursor.execute(f"SELECT COUNT(*) as total FROM movies {where_clause}", params)
            total_count = cursor.fetchone()['total']
            
            # PRIMARY SUCCESS PATH
            if total_count > 0 or (not title_id and not title and not region):
                cursor.execute(f"SELECT * FROM movies {where_clause} LIMIT %s OFFSET %s", params + [limit, offset])
                rows = cursor.fetchall()

                if not reader_autocommit:
                    conn.commit()
            
            # FALLBACK PATH 1: Requested node (N2/N3) found 0 results -> Try N1
            elif allow_fallback and requested_node != 'node1':
                conn.close()
                conn = None # Close initial conn and reset the variable

                # Attempt to connect to Node1
                conn_node1 = get_db_connection('node1', isolation_level=GLOBAL_SETTINGS['isolation_level'], autocommit_conn=True)
                
                if conn_node1:
                    cursor_node1 = conn_node1.cursor(dictionary=True)
                    cursor_node1.execute(f"SELECT COUNT(*) as total FROM movies {where_clause}", params)
                    total_count = cursor_node1.fetchone()['total']
                    cursor_node1.execute(f"SELECT * FROM movies {where_clause} LIMIT %s OFFSET %s", params + [limit, offset])
                    rows = cursor_node1.fetchall()
                    source = 'node1 (Fallback)'
                    # Note: We rely on autocommit=True used for conn_node1 here, no manual commit needed
                    
                else:
                    # N1 failed after N2/N3 returned zero results -> Trigger Aggregation
                    trigger_aggregation_fallback = True

        except Exception as e:
            # This block handles CONNECTION/QUERY FAILURE on the initial 'conn'
            print(f"Read Error on {target_node}: {e}")
            if not reader_autocommit:
                try: conn.rollback()
                except: pass
            
            # Close the failing initial connection
            if conn and conn.is_connected(): conn.close()
            conn = None

            # FALLBACK PATH 1 (Cont.): If N2/N3 failed due to error, try N1
            if requested_node != 'node1':
                
                # Attempt to connect to Node1
                conn_node1 = get_db_connection('node1', isolation_level=GLOBAL_SETTINGS['isolation_level'], autocommit_conn=True)
                
                if conn_node1:
                    try:
                        cursor_node1 = conn_node1.cursor(dictionary=True)
                        cursor_node1.execute(f"SELECT COUNT(*) as total FROM movies {where_clause}", params)
                        total_count = cursor_node1.fetchone()['total']
                        cursor_node1.execute(f"SELECT * FROM movies {where_clause} LIMIT %s OFFSET %s", params + [limit, offset])
                        rows = cursor_node1.fetchall()
                        source = 'node1 (Fallback)'
                    except Exception as e_node1:
                        print(f"Read Error on node1 fallback: {e_node1}")
                        trigger_aggregation_fallback = True
                    finally:
                        if conn_node1 and conn_node1.is_connected(): conn_node1.close()
                        conn_node1 = None # Mark as closed
                else:
                    # Node1 failed to connect -> Trigger Aggregation
                    trigger_aggregation_fallback = True
        
        # --- FALLBACK PATH 2: Aggregation from Fragments (If Node1 failed) ---
        if trigger_aggregation_fallback:
            source = 'Fragment Fallback (Aggregated)'
            
            # 2a. Fetch from Node2
            try:
                conn_for_node2 = get_db_connection('node2', isolation_level=GLOBAL_SETTINGS['isolation_level'], autocommit_conn=reader_autocommit)
                cursor_node2 = conn_for_node2.cursor(dictionary=True)
                cursor_node2.execute(f"SELECT COUNT(*) as total FROM movies {where_clause}", params)
                node2_count = cursor_node2.fetchone()['total']
                # Fetch ALL matching records (NO LIMIT/OFFSET) for correct in-memory pagination
                cursor_node2.execute(f"SELECT * FROM movies {where_clause}", params) 
                rows_node2 = cursor_node2.fetchall()
            except:
                node2_count = 0
                rows_node2 = []
            finally:
                if conn_for_node2 and conn_for_node2.is_connected() and not reader_autocommit:
                    try: conn_for_node2.commit()
                    except: pass

            # 2b. Fetch from Node3
            try:
                conn_for_node3 = get_db_connection('node3', isolation_level=GLOBAL_SETTINGS['isolation_level'], autocommit_conn=reader_autocommit)
                cursor_node3 = conn_for_node3.cursor(dictionary=True)
                cursor_node3.execute(f"SELECT COUNT(*) as total FROM movies {where_clause}", params)
                node3_count = cursor_node3.fetchone()['total']
                # Fetch ALL matching records (NO LIMIT/OFFSET) for correct in-memory pagination
                cursor_node3.execute(f"SELECT * FROM movies {where_clause}", params) 
                rows_node3 = cursor_node3.fetchall()
            except:
                node3_count = 0
                rows_node3 = []
            finally:
                if conn_for_node3 and conn_for_node3.is_connected() and not reader_autocommit:
                    try: conn_for_node3.commit()
                    except: pass
            
            # 2c. Combine and Paginate
            aggregated_rows = rows_node2 + rows_node3
            total_count = node2_count + node3_count
            
            # Apply LIMIT and OFFSET to the combined result set in memory
            rows = aggregated_rows[offset : offset + limit]

   
        # Close all connections
        if conn and conn.is_connected(): 
            conn.close()
        # conn_node1 is closed within the except block, but check for safety
        if conn_node1 and conn_node1.is_connected():
            conn_node1.close()
        if conn_for_node2 and conn_for_node2.is_connected(): 
            conn_for_node2.close()
        if conn_for_node3 and conn_for_node3.is_connected(): 
            conn_for_node3.close()
            
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

        # --- CONCURRENCY SIMULATION MODE (Auto Commit OFF) ---
        if not GLOBAL_SETTINGS['auto_commit']:
            print(f"Manual Mode: Locking {primary_target_node} AND node1")
            
            res_primary = execute_query(primary_target_node, query, params, commit_immediately=False)
            
            res_central = execute_query('node1', query, params, commit_immediately=False)
            
            if res_primary['success'] and res_central['success']:
                ACTIVE_TXN_CONNECTIONS[txn_id] = {
                    'type': 'INSERT', 
                    'status': 'PENDING_MANUAL',
                    'connections': { 
                        primary_target_node: res_primary['conn_obj'],
                        'node1': res_central['conn_obj']
                    },
                    'replication': {
                        'target': None, 
                        'query': query,
                        'params': params
                    }
                }
                return jsonify({
                    "status": "MANUAL_PENDING", 
                    "txn_id": txn_id, 
                    "logs": [f"Paused INSERT. Locked {primary_target_node} and Node 1 (Central)."]
                })
            else:
                if res_primary.get('conn_obj'): res_primary['conn_obj'].close()
                if res_central.get('conn_obj'): res_central['conn_obj'].close()
                return jsonify({"status": "FAILED", "error": "Failed to acquire locks on both nodes."})

        # --- RECOVERY V3 MODE (Auto Commit ON) ---
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
        region = data.get('region') 
        
        if not region:
            print(f"Region missing for {title_id}. Querying Central Node...")
            # Connect to Node 1
            conn_central = get_db_connection('node1', autocommit_conn=True)
            if conn_central:
                try:
                    cur = conn_central.cursor(dictionary=True)
                    cur.execute("SELECT region FROM movies WHERE titleId = %s", (title_id,))
                    row = cur.fetchone()
                    if row:
                        region = row['region']
                        print(f"Resolved region to: {region}")
                    else:
                        return jsonify({"error": "TitleID not found in Central Database."}), 404
                finally:
                    conn_central.close()
            else:
                return jsonify({"error": "Central Node Unavailable. Cannot determine region for routing."}), 500

        primary_target_node = 'node2' if region in ['US', 'JP'] else 'node3'

        params = (data.get('title'), data.get('ordering'), title_id)
        query = "UPDATE movies SET title = %s, ordering = %s WHERE titleId = %s"

        # --- CONCURRENCY SIMULATION MODE ---
        if not GLOBAL_SETTINGS['auto_commit']:
            res_primary = execute_query(primary_target_node, query, params, commit_immediately=False)
            res_central = execute_query('node1', query, params, commit_immediately=False)

            if res_primary['success'] and res_central['success']:
                ACTIVE_TXN_CONNECTIONS[txn_id] = {
                    'type': 'UPDATE', 
                    'status': 'PENDING_MANUAL',
                    'connections': { 
                        primary_target_node: res_primary['conn_obj'],
                        'node1': res_central['conn_obj']
                    },
                    'replication': {
                        'target': None, # No post-commit replication
                        'query': query,
                        'params': params
                    }
                }
                return jsonify({
                    "status": "MANUAL_PENDING", 
                    "txn_id": txn_id, 
                    "logs": [f"Paused UPDATE. Locked {primary_target_node} and Node 1."]
                })
            
            # Cleanup
            if res_primary.get('conn_obj'): res_primary['conn_obj'].close()
            if res_central.get('conn_obj'): res_central['conn_obj'].close()
            return jsonify({"status": "FAILED", "error": "Failed to acquire locks."})

        # --- RECOVERY V3 MODE ---
        replication_target_node = 'node1' if primary_target_node != 'node1' else None
        res_primary = execute_query(primary_target_node, query, params)
        LOG_MANAGER.log_local_commit(txn_id, 'UPDATE', title_id, data)
        if SIMULATE_CRASH_MODE: time.sleep(10)
        
        if not res_primary['success']:
            LOG_MANAGER.log_replication_attempt(txn_id, primary_target_node)
            LOG_MANAGER.update_replication_status(txn_id, 'REPLICATION_FAILED')

        if replication_target_node:
            execute_query(replication_target_node, query, params) 

        return jsonify({"status": "COMPLETED", "txn_id": txn_id})

    except Exception as e:
        traceback.print_exc()
        return jsonify({
            "status": "CRASH",
            "error": "Internal Server Error trapped",
            "details": str(e)
        }), 500

# --- DELETE (Combined Logic) ---
@app.route('/delete', methods=['POST'])
def delete_movie():
    try:
        data = request.json
        txn_id = str(uuid.uuid4())
        title_id = data.get('titleId')
        region = data.get('region') 
        
        if not region:
            conn_central = get_db_connection('node1', autocommit_conn=True)
            if conn_central:
                try:
                    cur = conn_central.cursor(dictionary=True)
                    cur.execute("SELECT region FROM movies WHERE titleId = %s", (title_id,))
                    row = cur.fetchone()
                    if row:
                        region = row['region']
                    else:
                        return jsonify({"error": "TitleID not found."}), 404
                finally:
                    conn_central.close()
            else:
                 return jsonify({"error": "Central Node Unavailable."}), 500
        
        primary_target_node = 'node2' if region in ['US', 'JP'] else 'node3'
        query = "DELETE FROM movies WHERE titleId = %s"
        params = (title_id,)

        # --- CONCURRENCY SIMULATION MODE ---
        if not GLOBAL_SETTINGS['auto_commit']:
            res_primary = execute_query(primary_target_node, query, params, commit_immediately=False)
            res_central = execute_query('node1', query, params, commit_immediately=False)

            if res_primary['success'] and res_central['success']:
                ACTIVE_TXN_CONNECTIONS[txn_id] = {
                    'type': 'DELETE', 
                    'status': 'PENDING_MANUAL',
                    'connections': { 
                        primary_target_node: res_primary['conn_obj'], 
                        'node1': res_central['conn_obj']
                    },
                    'replication': {
                        'target': None,
                        'query': query,
                        'params': params
                    }
                }
                return jsonify({
                    "status": "MANUAL_PENDING", 
                    "txn_id": txn_id, 
                    "logs": [f"Paused DELETE. Locked {primary_target_node} and Node 1."]
                })
            
            if res_primary.get('conn_obj'): res_primary['conn_obj'].close()
            if res_central.get('conn_obj'): res_central['conn_obj'].close()
            return jsonify({"status": "FAILED", "error": "Failed to acquire locks."})

        # --- RECOVERY V3 MODE ---
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
    if 'simulateBlocking' in data:
        val = data['simulateBlocking']
        if isinstance(val, str): val = val.lower() == 'true'
        GLOBAL_SETTINGS['simulate_blocking'] = val
    
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
    
    if action == 'COMMIT' and 'replication' in txn_info:
        repl = txn_info['replication']
        target = repl.get('target')
        
        if target:
            try:
                res = execute_query(target, repl['query'], repl['params'], commit_immediately=True)
                if res['success']:
                    logs.append(f"Replication to {target}: Success.")
                else:
                    logs.append(f"Replication to {target}: Failed ({res.get('error')}).")
            except Exception as e:
                logs.append(f"Replication Error: {str(e)}")
            
    del ACTIVE_TXN_CONNECTIONS[txn_id]
    return jsonify({"message": "Resolved", "logs": logs})

@app.route('/toggle-crash-mode', methods=['POST'])
def toggle_crash_mode():
    global SIMULATE_CRASH_MODE
    SIMULATE_CRASH_MODE = not SIMULATE_CRASH_MODE
    return jsonify({"status": "ENABLED" if SIMULATE_CRASH_MODE else "DISABLED"})

# Report #1 - Regional Distribution
@app.route('/report/distribution', methods=['GET'])
def report_distribution():
    """Generates Report 1: Count of movies per region"""
    target_node = request.args.get('node', 'node1')
    
    conn = get_db_connection(target_node)
    if not conn:
        return jsonify({"error": "Could not connect to node"}), 500

    try:
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT region, COUNT(*) as count 
            FROM movies 
            GROUP BY region 
            ORDER BY count DESC
        """
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Format as text report
        report_lines = [f"REPORT: Regional Distribution (Source: {target_node})", "="*50]
        report_lines.append(f"{'REGION':<15} | {'COUNT':<10}")
        report_lines.append("-" * 30)
        
        total = 0
        for row in results:
            r = row['region'] if row['region'] else 'Unknown'
            c = row['count']
            report_lines.append(f"{r:<15} | {c:<10}")
            total += c
            
        report_lines.append("-" * 30)
        report_lines.append(f"{'TOTAL':<15} | {total:<10}")
        
        return jsonify({"report": "\n".join(report_lines)})
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

# Report #2 - Content Type Breakdown
@app.route('/report/types', methods=['GET'])
def report_types():
    """Generates Report 2: Count of movies per content type"""
    target_node = request.args.get('node', 'node1')
    conn = get_db_connection(target_node)
    if not conn:
        return jsonify({"error": "Could not connect to node"}), 500

    try:
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT types, COUNT(*) as count 
            FROM movies 
            GROUP BY types 
            ORDER BY count DESC
        """
        cursor.execute(query)
        results = cursor.fetchall()
        
        report_lines = [f"REPORT: Content Type Breakdown (Source: {target_node})", "="*50]
        report_lines.append(f"{'TYPE':<20} | {'COUNT':<10}")
        report_lines.append("-" * 35)
        
        total = 0
        for row in results:
            t = row['types'] if row['types'] else 'Unknown'
            t_display = (t[:17] + '..') if len(t) > 17 else t
            c = row['count']
            report_lines.append(f"{t_display:<20} | {c:<10}")
            total += c
            
        report_lines.append("-" * 35)
        report_lines.append(f"{'TOTAL':<20} | {total:<10}")
        
        return jsonify({"report": "\n".join(report_lines)})
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

if __name__ == '__main__':
    start_background_recovery()
    app.run(host='0.0.0.0', port=80)