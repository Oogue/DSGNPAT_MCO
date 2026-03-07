import uuid
import time
import traceback
from flask import jsonify
from db_helpers import get_db_connection
from app import execute_query

class MovieUnitOfWork:
    def __init__(self, global_settings, log_manager, txn_connections, simulate_crash = False):
        self.settings = global_settings
        self.log_manager = log_manager
        self.simulate_crash = simulate_crash
        self.txn_connections = txn_connections
        self.txn_id = str(uuid.uuid4())
        self.operations = []
        self.primary_target_node = None
        self.replication_target_node = None

    @staticmethod
    def build_query(op_type, data, is_recovery=False): # Centralized query builder for all movie operations
        if op_type == 'INSERT':
            query = "INSERT INTO movies (titleId, ordering, title, region, language, types, attributes, isOriginalTitle) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            if is_recovery:
                query += " ON DUPLICATE KEY UPDATE title=title"
            params = (data.get('titleId'), data.get('ordering'), data.get('title'), data.get('region'), data.get('language'), data.get('types'), data.get('attributes'), data.get('isOriginalTitle'))
        elif op_type == 'UPDATE':
            query = "UPDATE movies SET title = %s, ordering = %s WHERE titleId = %s"
            params = (data.get('title'), data.get('ordering'), data.get('titleId'))
        elif op_type == 'DELETE':
            query = "DELETE FROM movies WHERE titleId = %s"
            params = (data.get('titleId'),)
        else:
            raise ValueError(f"Unknown operation type: {op_type}")
        return query, params

    def _resolve_region(self, title_id, region):
        conn_central = get_db_connection('node1', autocommit_conn=True)
        if not conn_central:
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
    
    def register(self, region, title_id, type, data):
        if not region:
            region = self._resolve_region(data.get('titleId'), data.get('region'))
        self.primary_target_node = 'node2' if region in ['US', 'JP'] else 'node3'

        query, params = MovieUnitOfWork.build_query(type, data)

        self.operations.append({
            'region': region,
            'title_id': title_id,
            'type': type,
            'data': data,
            'query': query,
            'params': params,   
        })

    def commit(self):
        logs = []
        try:
            for op in self.operations:
                # --- CONCURRENCY SIMULATION MODE (Auto Commit OFF) ---
                if not self.settings['auto_commit']:
                    res_primary = execute_query(self.primary_target_node, op['query'], op['params'], commit_immediately=False)
                    res_central = execute_query('node1', op['query'], op['params'], commit_immediately=False)

                    if res_primary['success'] and res_central ['success']:
                        self.txn_connections[self.txn_id] = {
                            'type': op['type'],
                            'status': 'PENDING_MANUAL',
                            'connections': {
                                self.primary_target_node: res_primary['conn_obj'],
                                'node1': res_central['conn_obj']
                            },
                            'replication': {
                                'target': None,
                                'query': op['query'],
                                'params': op['params']
                            }
                        }
                        return {
                            'status': 'MANUAL_PENDING',
                            'txn_id': self.txn_id,
                            'logs': [f"Paused INSERT. Locked {self.primary_target_node} and Node 1 (Central)."]
                        }
                    else:
                        if res_primary.get('conn_obj'): res_primary['conn_obj'].close()
                        if res_central.get('conn_obj'): res_central['conn_obj'].close()
                        return {"status": "FAILED", "error": "Failed to acquire locks on both nodes."}
                
                # --- RECOVERY V3 MODE ---
                self.replication_target_node = 'node1' if self.primary_target_node != 'node1' else None
                
                res_primary = execute_query(self.primary_target_node, op['query'], op['params'])
                self.log_manager.log_local_commit(self.txn_id, op['type'], op['data'])

                if self.simulate_crash: time.sleep(10)

                primary_success = res_primary['success']
                if not primary_success:
                    self.log_manager.log_replication_attempt(self.txn_id, self.primary_target_node)
                    self.log_manager.update_replication_status(self.txn_id, 'REPLICATION_SUCCESS')
                    logs.append(f"Primary {self.primary_target_node} Failed. Queued.")

                if self.replication_target_node:
                    res_replica = execute_query(self.replication_target_node, op['query'], op['params'])
                    if res_replica['success'] and primary_success:
                        self.log_manager.log_replication_attempt(self.txn_id, self.replication_target_node)
                        self.log_manager.update_replication_status(self.txn_id, 'REPLICATION_SUCCESS')
                    elif not res_replica['success']:
                        self.log_manager.log_replication_attempt(self.txn_id, self.replication_target_node)
                        self.log_manager.update_replication_status(self.txn_id, 'REPLICATION_FAILED')
                        logs.append(f"Replica {self.replication_target_node} Failed. Queued.")

                return {"status": "COMPLETED", "txn_id": self.txn_id, "logs": logs}
        
        except Exception as e:
            traceback.print_exc()
            return jsonify({
                "status": "CRASH",
                "error": "Internal Server Error trapped",
                "details": str(e)
            }), 500