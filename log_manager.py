import uuid
from datetime import datetime
import json
from db_helpers import get_db_connection

class DistributedLogManager:
    def __init__(self, node_id, db_connection):
        self.node_id = node_id  # 1 (Central), 2, or 3
        self.db_conn = db_connection
        self._initialize_log_table()

    def _initialize_log_table(self):
        """Creates the transaction_logs table if it doesn't exist."""
        sql = """
        CREATE TABLE IF NOT EXISTS transaction_logs (
            log_id INT AUTO_INCREMENT PRIMARY KEY,
            transaction_id VARCHAR(36) NOT NULL,
            log_timestamp DATETIME NOT NULL,
            operation_type VARCHAR(20), 
            record_key VARCHAR(50), 
            new_value JSON, 
            replication_target VARCHAR(10), 
            status VARCHAR(30) 
        );
        """
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(sql)
            self.db_conn.commit()
            cursor.close()
        except Exception as e:
            print(f"Error initializing log table: {e}")

       

    def log_local_commit(self, txn_id, op_type, key, new_data):
        """
        Logs that a transaction has been successfully committed LOCALLY.
        This is the 'Master' record for this transaction.
        """
        sql = """
        INSERT INTO transaction_logs 
        (transaction_id, log_timestamp, operation_type, record_key, new_value, status)
        VALUES (%s, %s, %s, %s, %s, 'LOCAL_COMMIT');
        """
        params = (
            txn_id,
            datetime.now(),
            op_type,
            key,
            json.dumps(new_data)
        )
        
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(sql, params)
            self.db_conn.commit() 
            cursor.close()
            print(f"Log: Transaction {txn_id} LOCALLY COMMITTED on Node {self.node_id}.")
            return True
        except Exception as e:
            print(f"FATAL LOGGING ERROR for {txn_id}: {e}")
            return False


    # --- Step 2: Logging Replication Attempts (Handles Case #1 and #3) ---

    def log_replication_attempt(self, txn_id, target_node_key):
        """
        Updates the log to indicate we are about to attempt replication.
        """
        sql = """
        UPDATE transaction_logs 
        SET status = 'REPLICATION_PENDING', replication_target = %s
        WHERE transaction_id = %s;
        """
        params = (target_node_key, txn_id)
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(sql, params)
            self.db_conn.commit()
            cursor.close()
        except Exception as e:
            print(f"Error logging replication attempt: {e}")
    
    def update_replication_status(self, txn_id, status):
        """
        Updates the status after a replication attempt (e.g., 'REPLICATION_SUCCESS' or 'REPLICATION_FAILED').
        """
        sql = """
        UPDATE transaction_logs 
        SET status = %s 
        WHERE transaction_id = %s;
        """
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(sql, (status, txn_id))
            self.db_conn.commit()
            cursor.close()
            print(f"Log: Transaction {txn_id} status updated to {status}.")
        except Exception as e:
            print(f"Error updating replication status: {e}")
    def get_failed_replications(self):
        """
        Retrieves transactions that were committed locally but failed (or got stuck) 
        during replication.
        """
        sql = """
        SELECT * FROM transaction_logs 
        WHERE status IN ('REPLICATION_FAILED', 'REPLICATION_PENDING', 'LOCAL_COMMIT')
        ORDER BY log_timestamp ASC;
        """
        # Note: We include 'LOCAL_COMMIT' in case the app crashed before it could even 
        # try to replicate.
        
        logs = []
        try:
            cursor = self.db_conn.cursor(dictionary=True)
            cursor.execute(sql)
            logs = cursor.fetchall()
            cursor.close()
        except Exception as e:
            print(f"Error fetching failed replications: {e}")
        
        return logs


