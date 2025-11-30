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

load_dotenv()
try:
    LOCAL_NODE_KEY = os.environ.get('LOCAL_NODE_KEY', 'node1') 
    LOCAL_NODE_ID = int(LOCAL_NODE_KEY.replace('node', ''))
    print(f"Local Node Key: {LOCAL_NODE_KEY}, ID: {LOCAL_NODE_ID}")
except Exception as e:
    print(f"Error determining local node from environment: {e}")
    LOCAL_NODE_KEY = 'node3'
    LOCAL_NODE_ID = 3
# Initialize Log Manager for Local Node
try:
    LOCAL_DB_CONN = mysql.connector.connect(**DB_CONFIG[LOCAL_NODE_KEY])
    LOG_MANAGER = DistributedLogManager(LOCAL_NODE_ID, LOCAL_DB_CONN)
    print(f"Log Manager initialized for {LOCAL_NODE_KEY}. Recovery startup complete.")
except Exception as e:
    print(f"Could not initialize Log Manager or connect to {LOCAL_NODE_KEY}: {e}")
    LOG_MANAGER = None

# Initialize the Flask application
app = Flask(__name__)
CORS(app)


# --- HELPER FUNCTION: Connect to DB ---
def get_db_connection(node_key):
    try:
        config = DB_CONFIG[node_key]
        conn = mysql.connector.connect(**config)
        return conn
    except Exception as e:
        print(f"Error connecting to {node_key}: {e}")
        return None
    
def execute_query(node_key, query, params=None):
    conn = get_db_connection(node_key)
    if not conn:
        return {"success": False, "error": "Connection failed"}
    
    try:
        cursor = conn.cursor()
        cursor.execute(query, params or ())
        conn.commit()
        rows_affected = cursor.rowcount
        cursor.close()
        conn.close()
        return {"success": True, "rows_affected": rows_affected}
    except Exception as e:
        return {"success": False, "error": str(e), "rows_affected": 0}

def get_row_count(node_key):
    """Get the total number of rows in a node"""
    conn = get_db_connection(node_key)
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
    """Get the timestamp of the last update in a node"""
    # TODO: Implement actual last update tracking
    # For now, return current timestamp
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
# --- NEW HELPER FUNCTIONS in app.py ---

def _prepare_write(node_key, query, params=None):
    """
    Phase 1: Executes the write query but DOES NOT commit. 
    It holds the transaction open until final_commit_or_abort is called.
    
    NOTE: For simplicity, we are committing the prepare log here, but the data 
    write is left uncommitted.
    """
    conn = get_db_connection(node_key)
    if not conn:
        return {"success": False, "error": "Connection failed"}
    
    try:
        # NOTE: A real system would use a distributed transaction manager to track 
        # this connection/transaction context. Here we rely on the connection object.
        cursor = conn.cursor()
        cursor.execute(query, params or ())
        
        rows_affected = cursor.rowcount
        cursor.close() 
        # Crucial: DO NOT conn.commit() here
        
        # Return the open connection to be managed by the calling route/coordinator
        return {"success": True, "rows_affected": rows_affected, "connection": conn}
    
    except Exception as e:
        if conn: conn.close()
        return {"success": False, "error": str(e), "rows_affected": 0}

def _final_commit_or_abort(conn, commit=True):
    """
    Phase 2: Performs the actual database commit or rollback based on 
    the coordinator's global decision.
    """
    if not conn:
        return {"success": False, "error": "No active connection/transaction"}
    
    try:
        if commit:
            conn.commit()
            status = "COMMIT_SUCCESS"
        else:
            conn.rollback()
            status = "ABORT_SUCCESS"
            
        conn.close()
        return {"success": True, "status": status}
    except Exception as e:
        conn.close()
        return {"success": False, "status": "FINAL_COMMIT_ERROR", "error": str(e)}

# NOTE: The original execute_query (which calls conn.commit()) is now redundant for 
# 2PC but is retained for old functions or read queries.    
    
    
# --- NEW HELPER: Core Recovery Logic ---
def _execute_recovery_cycle():
    """
    Contains the logic to scan and recover failed replications.
    Creates its own DB connection to ensure thread safety when called 
    from background threads.
    """
    # Create a fresh connection for this cycle
    conn = get_db_connection(LOCAL_NODE_KEY)
    if not conn:
        return {"success": False, "error": "Could not connect to local DB for recovery"}

    try:
        # Use a temporary Log Manager with the fresh connection
        temp_log_manager = DistributedLogManager(LOCAL_NODE_ID, conn)
        
        # 1. Get failed logs
        failed_txns = temp_log_manager.get_failed_replications()
        if not failed_txns:
            return {"success": True, "count": 0, "message": "No failed replications found."}
            
        recovery_logs = []
        recovered_count = 0
        
        for txn in failed_txns:
            txn_id = txn['transaction_id']
            target_node = txn['replication_target']
            payload = json.loads(txn['new_value'])
            op_type = txn['operation_type']
            
            # If target wasn't set, determine it now
            if not target_node:
                region = payload.get('region')
                if LOCAL_NODE_KEY == 'node1':
                    target_node = 'node2' if region in ['US', 'JP'] else 'node3'
                else:
                    target_node = 'node1'
            
            recovery_logs.append(f"Recovering Txn {txn_id} -> Target: {target_node}...")
            
            # Re-construct Query (Only supporting INSERT for this test)
            if op_type == 'INSERT':
                query = """
                    INSERT INTO movies 
                    (titleId, ordering, title, region, language, types, attributes, isOriginalTitle) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE title=title 
                """
                # ^ ON DUPLICATE KEY UPDATE makes it idempotent!
                
                params = (
                    payload.get('titleId'), payload.get('ordering'), payload.get('title'), 
                    payload.get('region'), payload.get('language'), payload.get('types'), 
                    payload.get('attributes'), payload.get('isOriginalTitle')
                )
                
                # Execute Retry
                res = execute_query(target_node, query, params)
                
                if res['success']:
                    temp_log_manager.update_replication_status(txn_id, 'REPLICATION_SUCCESS')
                    recovery_logs.append(f" -> Success: Replicated to {target_node}.")
                    recovered_count += 1
                else:
                    recovery_logs.append(f" -> Failed again: {res.get('error')}")
        
        return {
            "success": True,
            "count": recovered_count,
            "total_found": len(failed_txns),
            "details": recovery_logs
        }
        
    except Exception as e:
        print(f"Error in recovery cycle: {e}")
        return {"success": False, "error": str(e)}
    finally:
        conn.close()
        
# --- BACKGROUND TASK ---
def start_background_recovery():
    def task():
        print("Background Recovery Thread Started (Interval: 15s)")
        while True:
            time.sleep(15)
            try:
                # Silently run recovery
                result = _execute_recovery_cycle()
                if result['success'] and result.get('count', 0) > 0:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] AUTO-RECOVERY: Recovered {result['count']} transactions.")
            except Exception as e:
                print(f"Background Recovery Error: {e}")
                
    # Daemon thread ensures it dies when the main app stops
    thread = threading.Thread(target=task, daemon=True)
    thread.start()

# Frontend / Homepage
@app.route('/')
def index(): 
    return render_template('index.html')

# ROUTE: Status with detailed information
@app.route('/status', methods=['GET'])
def node_status():
    status_report = {}
    for key in DB_CONFIG:
        conn = get_db_connection(key)
        if conn:
            # TODO: Implement real-time status monitoring
            # - Check node health
            # - Monitor active connections
            # - Track transaction logs
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
    return jsonify(status_report)

# ROUTE: Read / Search with filters and pagination
@app.route('/movies', methods=['GET'])
def get_movies():
    # Get query parameters
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

    # 2. STRATEGY: Check Local Node First
    target_node = requested_node
    conn = get_db_connection(target_node)
    
    rows = []
    total_count = 0
    source = target_node

    # If connection works, try to fetch
    if conn:
        cursor = conn.cursor(dictionary=True)
        # Count
        cursor.execute(f"SELECT COUNT(*) as total FROM movies {where_clause}", params)
        total_count = cursor.fetchone()['total']
        
        # If local node has data OR if no filters are applied (browsing mode), use local
        # If local has 0 results BUT filters are applied, we might be looking for data in another node
        if total_count > 0 or (not title_id and not title and not region):
            cursor.execute(f"SELECT * FROM movies {where_clause} LIMIT %s OFFSET %s", params + [limit, offset])
            rows = cursor.fetchall()
            conn.close()
        else:
            # Local returned 0 results, but we are searching. 
            # 3. STRATEGY: Fallback to Central (Node 1) if we are on a fragment
            conn.close()
            if requested_node != 'node1':
                print(f"Search on {requested_node} yielded 0 results. Checking Central...")
                conn_central = get_db_connection('node1')
                if conn_central:
                    cursor_central = conn_central.cursor(dictionary=True)
                    cursor_central.execute(f"SELECT COUNT(*) as total FROM movies {where_clause}", params)
                    total_count = cursor_central.fetchone()['total']
                    cursor_central.execute(f"SELECT * FROM movies {where_clause} LIMIT %s OFFSET %s", params + [limit, offset])
                    rows = cursor_central.fetchall()
                    conn_central.close()
                    source = 'node1 (Fallback)'

    return jsonify({
        "data": rows,
        "total": total_count,
        "source_node": source
    })
    
# --- ROUTE: Recovery (Manual Trigger for Testing) ---
@app.route('/recover-replication', methods=['POST'])
def run_recovery():
    """
    Scans the local logs for failed replications and tries to re-apply them.
    This simulates a background recovery process.
    """
    if not LOG_MANAGER:
        return jsonify({"error": "Log Manager not ready"}), 500
        
    # 1. Get failed logs
    failed_txns = LOG_MANAGER.get_failed_replications()
    if not failed_txns:
        return jsonify({"message": "No failed replications found."})
        
    recovery_logs = []
    recovered_count = 0
    
    for txn in failed_txns:
        txn_id = txn['transaction_id']
        target_node = txn['replication_target']
        payload = json.loads(txn['new_value'])
        op_type = txn['operation_type']
        
        # If target wasn't set (e.g., crashed during local commit logging), determine it now
        if not target_node:
            region = payload.get('region')
            if LOCAL_NODE_KEY == 'node1':
                target_node = 'node2' if region in ['US', 'JP'] else 'node3'
            else:
                target_node = 'node1'
        
        recovery_logs.append(f"Recovering Txn {txn_id} -> Target: {target_node}...")
        
        # Re-construct Query (Only supporting INSERT for this test)
        if op_type == 'INSERT':
            query = """
                INSERT INTO movies 
                (titleId, ordering, title, region, language, types, attributes, isOriginalTitle) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE title=title 
            """
            # ^ ON DUPLICATE KEY UPDATE makes it idempotent! Important for recovery.
            
            params = (
                payload.get('titleId'), payload.get('ordering'), payload.get('title'), 
                payload.get('region'), payload.get('language'), payload.get('types'), 
                payload.get('attributes'), payload.get('isOriginalTitle')
            )
            
            # Execute Retry
            res = execute_query(target_node, query, params)
            
            if res['success']:
                LOG_MANAGER.update_replication_status(txn_id, 'REPLICATION_SUCCESS')
                recovery_logs.append(f" -> Success: Replicated to {target_node}.")
                recovered_count += 1
            else:
                recovery_logs.append(f" -> Failed again: {res.get('error')}")
    
    return jsonify({
        "message": f"Recovery run complete. Recovered {recovered_count}/{len(failed_txns)} items.",
        "details": recovery_logs
    })


# --- ROUTE: Insert (Local Commit First Strategy) ---
@app.route('/insert', methods=['POST'])
def insert_movie():
    if not LOG_MANAGER:
        return jsonify({"error": "Distributed Log Manager not initialized."}), 500

    data = request.json
    
    # 1. Transaction Setup
    txn_id = str(uuid.uuid4())
    
    # Prepare Data
    title_id = data.get('titleId')
    region = data.get('region')
    
    params = (
        title_id, data.get('ordering'), data.get('title'), region, 
        data.get('language'), data.get('types'), data.get('attributes'), data.get('isOriginalTitle')
    )
    
    # Define the Query
    query = """
        INSERT INTO movies 
        (titleId, ordering, title, region, language, types, attributes, isOriginalTitle) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    # 2. Determine Logic based on Location (Fragmentation Rules)
    # Rules:
    # - US/JP -> Stored in Node 2
    # - Others -> Stored in Node 3
    # - Node 1 -> Stores EVERYTHING (Central)
    
    target_fragment = 'node2' if region in ['US', 'JP'] else 'node3'
    
    # Identify "Local" vs "Remote"
    # The 'primary_target' is where the data MUST exist.
    # If we are Node 1, we save locally, then replicate to fragment.
    # If we are Fragment, we save locally, then replicate to Node 1.
    
    replication_target_node = None
    
    if LOCAL_NODE_KEY == 'node1':
        # We are Central. 
        # Local Write: YES.
        # Remote Write: To the specific fragment.
        replication_target_node = target_fragment
    else:
        # We are a Fragment (Node 2 or 3).
        # Check if this data actually belongs to us.
        if LOCAL_NODE_KEY == target_fragment:
            # It belongs here.
            # Local Write: YES.
            # Remote Write: To Central (Node 1).
            replication_target_node = 'node1'
        else:
            # Edge Case: User sent a "US" movie to Node 3.
            # In a strict system, we might reject or forward. 
            # For this test, we will try to write to Node 3 (as misplaced data) 
            # OR we can reject. Let's assume we proceed but replicate to Central.
            replication_target_node = 'node1'

    logs = []
    
    # --- PHASE 1: LOCAL COMMIT ---
    logs.append(f"Step 1: Attempting LOCAL COMMIT to {LOCAL_NODE_KEY}...")
    
    local_res = execute_query(LOCAL_NODE_KEY, query, params)
    
    if not local_res['success']:
        # If local commit fails, the whole thing fails. Client receives error.
        return jsonify({
            "status": "FAILURE", 
            "stage": "LOCAL_COMMIT", 
            "error": local_res['error'],
            "logs": logs
        }), 500
    
    # Log the successful local commit to DB
    # We store the 'new_value' in JSON for recovery purposes later
    new_value = data
    LOG_MANAGER.log_local_commit(txn_id, 'INSERT', title_id, new_value)
    logs.append("Step 1 Success: Local DB write & Log Entry created.")

    # --- PHASE 2: REPLICATION (Best Effort) ---
    logs.append(f"Step 2: Attempting REPLICATION to {replication_target_node}...")
    
    # Update log status to PENDING
    LOG_MANAGER.log_replication_attempt(txn_id, replication_target_node)
    
    # Attempt Remote Write
    remote_res = execute_query(replication_target_node, query, params)
    
    if remote_res['success']:
        # Update Log to SUCCESS
        LOG_MANAGER.update_replication_status(txn_id, 'REPLICATION_SUCCESS')
        logs.append(f"Step 2 Success: Replicated to {replication_target_node}.")
        final_status = "FULLY_COMMITTED"
    else:
        # Update Log to FAILED
        LOG_MANAGER.update_replication_status(txn_id, 'REPLICATION_FAILED')
        logs.append(f"Step 2 Failed: Could not write to {replication_target_node}. Error: {remote_res.get('error')}")
        logs.append("Action: Marked as REPLICATION_FAILED in logs. Background recovery will handle it.")
        final_status = "LOCAL_ONLY_REPLICATION_PENDING"

    return jsonify({
        "status": final_status,
        "txn_id": txn_id,
        "logs": logs,
        "local_rows": local_res['rows_affected'],
        "replication_error": remote_res.get('error') if not remote_res['success'] else None
    })


# ROUTE: Update (Refactored for 2PC)
@app.route('/update', methods=['POST'])
def update_movie():
    # Ensure the LOG_MANAGER is available (from global instantiation)
    if not LOG_MANAGER:
        return jsonify({"error": "Distributed Log Manager not initialized."}), 500

    data = request.json
    current_node = request.args.get('node', data.get('node', 'node1'))
    
    # 1. Transaction Setup & Log Data Preparation
    txn_id = str(uuid.uuid4()) 
    record_key = data.get('titleId')
    title_id = data.get('titleId')
    new_title = data.get('title')
    new_ordering = data.get('ordering')

    # Prepare the 'new_value' payload for the log (After Image)
    new_value = {
        'titleId': record_key,
        'ordering': new_ordering,
        'title': new_title,
        # IMPORTANT: Include other updated fields if necessary for REDO
    }
    
    query = "UPDATE movies SET title = %s, ordering = %s WHERE titleId = %s"
    params = (new_title, new_ordering, title_id)
    
    logs = []

    # --- 1. IDENTIFY ALL PARTICIPANTS (REQUIRED STEP FOR 2PC) ---
    participants = set()
    # Central node always participates (or is the entry point)
    participants.add('node1') 
    # Both fragment nodes must participate in an UPDATE, as the location is unknown
    participants.add('node2') 
    participants.add('node3')
    
    # Add the current coordinator node if it's not already in the set (it will be)
    participants.add(current_node)

    active_connections = {}
    all_ready = True
    
    # ------------------------------------------------------------------
    # PHASE 1: PREPARE AND LOG READY STATUS (THE LOOP)
    # ------------------------------------------------------------------
    try:
        # Coordinator logs PREPARE START
        LOG_MANAGER.log_prepare_start(txn_id)
        logs.append("Coordinator: Logged PREPARE START.")
        
        # --- THE REQUIRED LOOP ITERATING OVER ALL PARTICIPANTS ---
        for p_key in participants:
            # --- 1. Perform DB Write (NO COMMIT) ---
            # NOTE: We use the prepare_write helper (which manages the connection)
            res_prepare = _prepare_write(p_key, query, params)
            
            if res_prepare['success']:
                # 2. Log READY status on the coordinator's log for each successful prepare
                LOG_MANAGER.log_ready_status(txn_id, 'UPDATE', record_key, new_value)
                logs.append(f"{p_key}: Prepared write & Logged READY_COMMIT (Transaction held).")
                active_connections[p_key] = res_prepare['connection'] # Save the open connection
            else:
                # One participant failed to prepare. Global abort is inevitable.
                logs.append(f"{p_key}: Failed to Prepare: {res_prepare.get('error')}. ABORTING.")
                all_ready = False
                # Immediately close failed connection
                if 'connection' in res_prepare: _final_commit_or_abort(res_prepare['connection'], commit=False)
                break
        
    except Exception as e:
        all_ready = False
        logs.append(f"CRITICAL FAILURE during PREPARE phase: {e}")

    # ------------------------------------------------------------------
    # PHASE 2: GLOBAL COMMIT/ABORT DECISION (THE SECOND LOOP)
    # ------------------------------------------------------------------
    final_decision = all_ready
    
    # 1. Coordinator logs GLOBAL_COMMIT/ABORT (The irrevocable decision)
    log_res = LOG_MANAGER.log_global_commit(txn_id, commit=final_decision)
    if not log_res['success']:
        # This is a critical logging failure. Must force abort.
        final_decision = False 
        logs.append("CRITICAL: Global Log Failure. FORCING ABORT.")
        
    # 2. Coordinator sends final commit/abort signal to all open connections
    for node_key, conn in active_connections.items():
        commit_res = _final_commit_or_abort(conn, commit=final_decision)
        logs.append(f"{node_key}: Final Decision - {'COMMIT' if final_decision else 'ABORT'} ({commit_res['status']})")
        
    return jsonify({
        "message": "Update Processed via 2PC", 
        "decision": "COMMITTED" if final_decision else "ABORTED",
        "logs": logs,
        "txn_id": txn_id
    })
    
    
# ROUTE: Delete (Refactored for 2PC)
@app.route('/delete', methods=['POST'])
def delete_movie():
    # Ensure the LOG_MANAGER is available
    if not LOG_MANAGER:
        return jsonify({"error": "Distributed Log Manager not initialized."}), 500

    data = request.json
    current_node = request.args.get('node', data.get('node', 'node1'))
    
    # 1. Transaction Setup & Log Data Preparation
    txn_id = str(uuid.uuid4())
    record_key = data.get('titleId')
    title_id = data.get('titleId')
    
    # For a DELETE operation (REDO only), we log the key and the operation type.
    new_value = {"action": "DELETE", "titleId": title_id}

    query = "DELETE FROM movies WHERE titleId = %s"
    params = (title_id,)
    
    logs = []

    # --- 1. IDENTIFY ALL PARTICIPANTS (REQUIRED STEP FOR 2PC) ---
    participants = set()
    # Central node always participates
    participants.add('node1') 
    # Both fragment nodes must participate in a DELETE to ensure the record is removed everywhere
    participants.add('node2') 
    participants.add('node3')
    
    # The coordinating node must also be in the set
    participants.add(current_node)

    active_connections = {}
    all_ready = True
    
    # ------------------------------------------------------------------
    # PHASE 1: PREPARE AND LOG READY STATUS (THE LOOP)
    # ------------------------------------------------------------------
    try:
        # Coordinator logs PREPARE START
        LOG_MANAGER.log_prepare_start(txn_id)
        logs.append("Coordinator: Logged PREPARE START.")
        
        # --- THE REQUIRED LOOP ITERATING OVER ALL PARTICIPANTS ---
        for p_key in participants:
            # --- 1. Perform DB Write (NO COMMIT) ---
            # This executes the DELETE statement but holds the transaction open
            res_prepare = _prepare_write(p_key, query, params)
            
            if res_prepare['success']:
                # 2. Log READY status on the coordinator's log for each successful prepare
                LOG_MANAGER.log_ready_status(txn_id, 'DELETE', record_key, new_value)
                logs.append(f"{p_key}: Prepared delete & Logged READY_COMMIT (Transaction held).")
                active_connections[p_key] = res_prepare['connection'] # Save the open connection
            else:
                # One participant failed to prepare. Global abort is inevitable.
                logs.append(f"{p_key}: Failed to Prepare: {res_prepare.get('error')}. ABORTING.")
                all_ready = False
                # Immediately close failed connection
                if 'connection' in res_prepare: _final_commit_or_abort(res_prepare['connection'], commit=False)
                break
        
    except Exception as e:
        all_ready = False
        logs.append(f"CRITICAL FAILURE during PREPARE phase: {e}")

    # ------------------------------------------------------------------
    # PHASE 2: GLOBAL COMMIT/ABORT DECISION (THE SECOND LOOP)
    # ------------------------------------------------------------------
    final_decision = all_ready
    
    # 1. Coordinator logs GLOBAL_COMMIT/ABORT (The irrevocable decision)
    log_res = LOG_MANAGER.log_global_commit(txn_id, commit=final_decision)
    if not log_res['success']:
        # This is a critical logging failure. Must force abort.
        final_decision = False 
        logs.append("CRITICAL: Global Log Failure. FORCING ABORT.")
        
    # 2. Coordinator sends final commit/abort signal to all open connections
    for node_key, conn in active_connections.items():
        commit_res = _final_commit_or_abort(conn, commit=final_decision)
        logs.append(f"{node_key}: Final Decision - {'COMMIT' if final_decision else 'ABORT'} ({commit_res['status']})")
        
    return jsonify({
        "message": "Delete Processed via 2PC", 
        "decision": "COMMITTED" if final_decision else "ABORTED",
        "logs": logs,
        "txn_id": txn_id
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

# ROUTE: Report #1 - Regional Distribution
@app.route('/report/distribution', methods=['GET'])
def report_distribution():
    """Generates Report 1: Count of movies per region"""
    target_node = request.args.get('node', 'node1')
    
    # We query the target node directly to see what IT sees
    conn = get_db_connection(target_node)
    if not conn:
        return jsonify({"error": "Could not connect to node"}), 500

    try:
        cursor = conn.cursor(dictionary=True)
        # Simple aggregation query
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

# ROUTE: Report #2 - Content Type Breakdown
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
            # Truncate long types for text display
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