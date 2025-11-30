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


SIMULATE_CRASH_MODE = False

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
    

    
def _execute_recovery_cycle():
    conn = get_db_connection(LOCAL_NODE_KEY)
    if not conn:
        return {"success": False, "error": "Could not connect to local DB for recovery"}

    try:
        temp_log_manager = DistributedLogManager(LOCAL_NODE_ID, conn)
        failed_txns = temp_log_manager.get_failed_replications()
        if not failed_txns:
            return {"success": True, "count": 0, "message": "No failed replications found."}
            
        recovery_logs = []
        recovered_count = 0
        
        for txn in failed_txns:
            txn_id = txn['transaction_id']
            target_node = txn['replication_target']
            try:
                payload = json.loads(txn['new_value'])
            except:
                payload = {}
                
            op_type = txn['operation_type']
            region = payload.get('region')
            
            # --- IMPROVED TARGET LOGIC ---
            if not target_node or str(target_node) == '0':
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

            recovery_logs.append(f"Recovering Txn {txn_id} ({op_type}) -> Target: {target_node}...")
            
            res = {'success': False, 'error': 'Unknown Op'}

            # --- RECOVERY HANDLER: INSERT ---
            if op_type == 'INSERT':
                query = """
                    INSERT INTO movies 
                    (titleId, ordering, title, region, language, types, attributes, isOriginalTitle) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE title=title 
                """
                params = (
                    payload.get('titleId'), payload.get('ordering'), payload.get('title'), 
                    payload.get('region'), payload.get('language'), payload.get('types'), 
                    payload.get('attributes'), payload.get('isOriginalTitle')
                )
                res = execute_query(target_node, query, params)

            # --- RECOVERY HANDLER: UPDATE ---
            elif op_type == 'UPDATE':
                query = "UPDATE movies SET title = %s, ordering = %s WHERE titleId = %s"
                params = (
                    payload.get('title'), 
                    payload.get('ordering'), 
                    payload.get('titleId')
                )
                res = execute_query(target_node, query, params)

            # --- RECOVERY HANDLER: DELETE ---
            elif op_type == 'DELETE':
                query = "DELETE FROM movies WHERE titleId = %s"
                params = (payload.get('titleId'),)
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
        if conn: conn.close()
        
        
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

# --- ROUTE: Insert (Failover Logic) ---
@app.route('/insert', methods=['POST'])
def insert_movie():
    try:
        if not LOG_MANAGER:
            return jsonify({"error": "Distributed Log Manager not initialized."}), 503
        if not request.json:
             return jsonify({"error": "Missing JSON body"}), 400

        data = request.json
        txn_id = str(uuid.uuid4())
        
        title_id = data.get('titleId')
        region = data.get('region')
        
        print(f"\n--- PROCESSING INSERT: {title_id} ({region}) ---")
        
        params = (
            title_id, data.get('ordering'), data.get('title'), region, 
            data.get('language'), data.get('types'), data.get('attributes'), data.get('isOriginalTitle')
        )
        
        query = """
            INSERT INTO movies 
            (titleId, ordering, title, region, language, types, attributes, isOriginalTitle) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        # 1. Determine Nodes
        primary_target_node = 'node2' if region in ['US', 'JP'] else 'node3'
        replication_target_node = 'node1' if primary_target_node != 'node1' else None
        
        print(f"   [DECISION] Primary: {primary_target_node}, Secondary: {replication_target_node}")

        logs = []
        
        # --- PHASE 1: PRIMARY COMMIT ---
        logs.append(f"Step 1: Attempting PRIMARY COMMIT to {primary_target_node}...")
        res_primary = execute_query(primary_target_node, query, params)
        
        # Log the transaction record immediately (Local Log)
        new_value = data
        LOG_MANAGER.log_local_commit(txn_id, 'INSERT', title_id, new_value)
        # --- CRASH SIMULATION POINT ---
        if SIMULATE_CRASH_MODE:
            print("\n!!! CRASH MODE ACTIVE !!!")
            print("!!! YOU HAVE 10 SECONDS TO KILL THE SERVER TO SIMULATE A CRASH AFTER LOCAL COMMIT !!!")
            time.sleep(10)
            
        primary_success = False
        if res_primary['success']:
            primary_success = True
            logs.append(f"Step 1 Success: Written to {primary_target_node}.")
        else:
            # IMPORTANT: Primary Failed. 
            # We UPDATE the log to point to the FAILED primary target.
            # This ensures the Recovery Thread sees "Target: node3, Status: FAILED" and retries later.
            primary_success = False
            logs.append(f"Step 1 Failed: {res_primary.get('error')}")
            
            LOG_MANAGER.log_replication_attempt(txn_id, primary_target_node)
            LOG_MANAGER.update_replication_status(txn_id, 'REPLICATION_FAILED')
            
            logs.append(f"Action: Log updated. Recovery will retry {primary_target_node} later.")

        # --- PHASE 2: REPLICATION / BACKUP (To Central) ---
        final_status = "FULLY_COMMITTED"
        
        if replication_target_node:
            logs.append(f"Step 2: Proceeding to Node 1 ({replication_target_node})...")
            
            res_replica = execute_query(replication_target_node, query, params)
            
            if res_replica['success']:
                logs.append(f"Step 2 Success: Data saved on {replication_target_node}.")
                
                if primary_success:
                    # Normal Case: Primary OK, Replica OK.
                    # Update log to reflect replication success.
                    LOG_MANAGER.log_replication_attempt(txn_id, replication_target_node)
                    LOG_MANAGER.update_replication_status(txn_id, 'REPLICATION_SUCCESS')
                else:
                    # Failover Case: Primary FAILED, Replica OK.
                    # DO NOT update log target. Leave it pointing to FAILED Primary so recovery fixes it.
                    final_status = "PRIMARY_FAILED_BACKUP_SUCCESS"
                    logs.append("Info: Transaction saved on Backup Node only. Primary recovery pending.")
            else:
                logs.append(f"Step 2 Failed: {res_replica.get('error')}")
                
                if primary_success:
                    # Primary OK, Replica Failed. Mark Replica for recovery.
                    LOG_MANAGER.log_replication_attempt(txn_id, replication_target_node)
                    LOG_MANAGER.update_replication_status(txn_id, 'REPLICATION_FAILED')
                    final_status = "PRIMARY_ONLY_REPLICATION_PENDING"
                else:
                    # Both Failed. 
                    final_status = "TOTAL_FAILURE_PENDING_RECOVERY"

        return jsonify({
            "status": final_status,
            "txn_id": txn_id,
            "logs": logs,
            "primary_node": primary_target_node,
            "replication_node": replication_target_node
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({
            "status": "CRASH",
            "error": "Internal Server Error trapped",
            "details": str(e)
        }), 500

# --- ROUTE: Update (Failover Logic) ---
@app.route('/update', methods=['POST'])
def update_movie():
    try:
        if not LOG_MANAGER:
            return jsonify({"error": "Distributed Log Manager not initialized."}), 503
        if not request.json:
             return jsonify({"error": "Missing JSON body"}), 400

        data = request.json
        txn_id = str(uuid.uuid4())
        
        title_id = data.get('titleId')
        # We assume region is passed for routing; if not, we might default to Node 3 or need logic
        region = data.get('region') 
        new_title = data.get('title')
        new_ordering = data.get('ordering')
        
        print(f"\n--- PROCESSING UPDATE: {title_id} ---")
        
        params = (new_title, new_ordering, title_id)
        
        query = "UPDATE movies SET title = %s, ordering = %s WHERE titleId = %s"

        # 1. Determine Nodes (Same logic as Insert)
        # If region is missing, this logic might default to node3. 
        # In a real app, we'd query Central to find the region first, but that adds latency.
        primary_target_node = 'node2' if region in ['US', 'JP'] else 'node3'
        replication_target_node = 'node1' if primary_target_node != 'node1' else None
        
        print(f"   [DECISION] Primary: {primary_target_node}, Secondary: {replication_target_node}")

        logs = []
        
        # --- PHASE 1: PRIMARY COMMIT ---
        logs.append(f"Step 1: Attempting PRIMARY UPDATE to {primary_target_node}...")
        res_primary = execute_query(primary_target_node, query, params)
        
        # Log the transaction
        # Important: Store all data needed for REDO in 'new_value'
        new_value = {
            'titleId': title_id,
            'ordering': new_ordering,
            'title': new_title,
            'region': region # Stored for recovery routing
        }
        LOG_MANAGER.log_local_commit(txn_id, 'UPDATE', title_id, new_value)
        # --- CRASH SIMULATION POINT ---
        if SIMULATE_CRASH_MODE:
            print("\n!!! CRASH MODE ACTIVE !!!")
            print("!!! YOU HAVE 10 SECONDS TO KILL THE SERVER TO SIMULATE A CRASH AFTER LOCAL COMMIT !!!")
            time.sleep(10)
            
        primary_success = False
        if res_primary['success']:
            primary_success = True
            logs.append(f"Step 1 Success: Updated {primary_target_node}.")
        else:
            primary_success = False
            logs.append(f"Step 1 Failed: {res_primary.get('error')}")
            
            # Point log to failed primary
            LOG_MANAGER.log_replication_attempt(txn_id, primary_target_node)
            LOG_MANAGER.update_replication_status(txn_id, 'REPLICATION_FAILED')
            
            logs.append(f"Action: Log updated. Recovery will retry {primary_target_node} later.")

        # --- PHASE 2: REPLICATION / BACKUP ---
        final_status = "FULLY_COMMITTED"
        
        if replication_target_node:
            logs.append(f"Step 2: Proceeding to Node 1 ({replication_target_node})...")
            
            res_replica = execute_query(replication_target_node, query, params)
            
            if res_replica['success']:
                logs.append(f"Step 2 Success: Updated {replication_target_node}.")
                
                if primary_success:
                    LOG_MANAGER.log_replication_attempt(txn_id, replication_target_node)
                    LOG_MANAGER.update_replication_status(txn_id, 'REPLICATION_SUCCESS')
                else:
                    final_status = "PRIMARY_FAILED_BACKUP_SUCCESS"
                    logs.append("Info: Transaction saved on Backup Node only. Primary recovery pending.")
            else:
                logs.append(f"Step 2 Failed: {res_replica.get('error')}")
                
                if primary_success:
                    LOG_MANAGER.log_replication_attempt(txn_id, replication_target_node)
                    LOG_MANAGER.update_replication_status(txn_id, 'REPLICATION_FAILED')
                    final_status = "PRIMARY_ONLY_REPLICATION_PENDING"
                else:
                    final_status = "TOTAL_FAILURE_PENDING_RECOVERY"

        return jsonify({
            "status": final_status,
            "txn_id": txn_id,
            "logs": logs,
            "primary_node": primary_target_node,
            "replication_node": replication_target_node
        })

    except Exception as e:
        traceback.print_exc()
        return jsonify({
            "status": "CRASH",
            "error": "Internal Server Error trapped",
            "details": str(e)
        }), 500
    
    # --- ROUTE: Delete (Failover Logic) ---

@app.route('/delete', methods=['POST'])
def delete_movie():
    try:
        if not LOG_MANAGER:
            return jsonify({"error": "Distributed Log Manager not initialized."}), 503
        if not request.json:
             return jsonify({"error": "Missing JSON body"}), 400

        data = request.json
        txn_id = str(uuid.uuid4())
        
        title_id = data.get('titleId')
        # We assume region is passed for routing to determine primary
        region = data.get('region') 
        
        print(f"\n--- PROCESSING DELETE: {title_id} ---")
        
        params = (title_id,)
        
        query = "DELETE FROM movies WHERE titleId = %s"

        # 1. Determine Nodes (Same logic as Insert/Update)
        primary_target_node = 'node2' if region in ['US', 'JP'] else 'node3'
        replication_target_node = 'node1' if primary_target_node != 'node1' else None
        
        print(f"   [DECISION] Primary: {primary_target_node}, Secondary: {replication_target_node}")

        logs = []
        
        # --- PHASE 1: PRIMARY COMMIT ---
        logs.append(f"Step 1: Attempting PRIMARY DELETE on {primary_target_node}...")
        res_primary = execute_query(primary_target_node, query, params)
        
        # Log the transaction
        new_value = {
            'action': 'DELETE',
            'titleId': title_id,
            'region': region 
        }
        LOG_MANAGER.log_local_commit(txn_id, 'DELETE', title_id, new_value)
        # --- CRASH SIMULATION POINT ---
        if SIMULATE_CRASH_MODE:
            print("\n!!! CRASH MODE ACTIVE !!!")
            print("!!! YOU HAVE 10 SECONDS TO KILL THE SERVER TO SIMULATE A CRASH AFTER LOCAL COMMIT !!!")
            time.sleep(10)
            
        primary_success = False
        if res_primary['success']:
            primary_success = True
            logs.append(f"Step 1 Success: Deleted from {primary_target_node}.")
        else:
            primary_success = False
            logs.append(f"Step 1 Failed: {res_primary.get('error')}")
            
            # Point log to failed primary
            LOG_MANAGER.log_replication_attempt(txn_id, primary_target_node)
            LOG_MANAGER.update_replication_status(txn_id, 'REPLICATION_FAILED')
            
            logs.append(f"Action: Log updated. Recovery will retry {primary_target_node} later.")

        # --- PHASE 2: REPLICATION / BACKUP ---
        final_status = "FULLY_COMMITTED"
        
        if replication_target_node:
            logs.append(f"Step 2: Proceeding to Node 1 ({replication_target_node})...")
            
            res_replica = execute_query(replication_target_node, query, params)
            
            if res_replica['success']:
                logs.append(f"Step 2 Success: Deleted from {replication_target_node}.")
                
                if primary_success:
                    LOG_MANAGER.log_replication_attempt(txn_id, replication_target_node)
                    LOG_MANAGER.update_replication_status(txn_id, 'REPLICATION_SUCCESS')
                else:
                    final_status = "PRIMARY_FAILED_BACKUP_SUCCESS"
                    logs.append("Info: Transaction saved on Backup Node only. Primary recovery pending.")
            else:
                logs.append(f"Step 2 Failed: {res_replica.get('error')}")
                
                if primary_success:
                    LOG_MANAGER.log_replication_attempt(txn_id, replication_target_node)
                    LOG_MANAGER.update_replication_status(txn_id, 'REPLICATION_FAILED')
                    final_status = "PRIMARY_ONLY_REPLICATION_PENDING"
                else:
                    final_status = "TOTAL_FAILURE_PENDING_RECOVERY"

        return jsonify({
            "status": final_status,
            "txn_id": txn_id,
            "logs": logs,
            "primary_node": primary_target_node,
            "replication_node": replication_target_node
        })

    except Exception as e:
        traceback.print_exc()
        return jsonify({
            "status": "CRASH",
            "error": "Internal Server Error trapped",
            "details": str(e)
        }), 500

    
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

# --- ROUTE: Toggle Crash Simulation (NEW) ---
@app.route('/toggle-crash-mode', methods=['POST'])
def toggle_crash_mode():
    global SIMULATE_CRASH_MODE
    SIMULATE_CRASH_MODE = not SIMULATE_CRASH_MODE
    status = "ENABLED" if SIMULATE_CRASH_MODE else "DISABLED"
    print(f"!!! CRASH SIMULATION MODE {status} !!!")
    return jsonify({"status": status, "mode": SIMULATE_CRASH_MODE})

if __name__ == '__main__':
    start_background_recovery()
    app.run(host='0.0.0.0', port=80)