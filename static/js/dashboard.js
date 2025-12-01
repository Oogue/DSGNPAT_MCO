let currentNode = null;

function navigateToNode(nodeNumber) {
    const config = document.getElementById('app-config');
    const allowedNode = parseInt(config.getAttribute('data-current-node-id'));
    
    // Only allow viewing the current server's node
    if (nodeNumber !== allowedNode) {
        alert(`You can only view ${config.getAttribute('data-current-node')} from this server.`);
        return;
    }
    
    currentNode = nodeNumber;
    //document.getElementById('dashboard-view').style.display = 'none';
    document.getElementById('node-view').classList.add('active');
    //document.getElementById('current-node-title').textContent = 
    //    nodeNumber === 1 ? 'Node 1 (Central)' : `Node ${nodeNumber} (Regional)`;
    
    loadNodeData(nodeNumber);
}

function navigateToDashboard() {
    // Disabled - dashboard view not accessible in single-node mode
    alert("Dashboard view is not available. Each server shows only its own node.");
}

async function applySettings() {
    const isolationLevel = document.getElementById('isolation-level').value;
    const autoCommitToggle = document.getElementById('autocommit-toggle').checked;
    const blockingToggle = document.getElementById('blocking-toggle').checked;
    
    console.log('Applying settings:', { isolationLevel, autoCommit: autoCommitToggle, simulateBlocking: blockingToggle });
    
    try {
        const response = await fetch('/settings', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                isolationLevel: isolationLevel,
                autoCommit: autoCommitToggle,
                simulateBlocking: blockingToggle
            })
        });
        
        const result = await response.json();
        const readMode = blockingToggle ? "Locking (May Freeze)" : "Fast (Non-Blocking)";
        alert(`Settings Applied!\nIsolation: ${isolationLevel}\nWrite Mode: ${autoCommitToggle ? "Auto" : "Manual"}\nRead Mode: ${readMode}`);
        
    } catch (error) {
        console.error("Error sending settings:", error);
        alert("Failed to apply settings.");
    }
    
    updateCommitButtonState();
}

function updateCommitButtonState() {
    const toggle = document.getElementById('autocommit-toggle');
    const btn = document.getElementById('commit-changes-btn');
    
    if (toggle.checked) {
        // Auto Commit is ON -> Disable Manual Commit Button
        btn.disabled = true;
        btn.title = "Switch to Manual Mode to use this button";
    } else {
        // Manual Mode is ON -> Enable Button
        btn.disabled = false;
        btn.title = "View pending transactions";
    }
}

// --- NEW FUNCTIONS FOR COMMIT MODAL ---

function openCommitModal() {
    document.getElementById('commit-modal').classList.add('active');
    refreshPendingTransactions();
}

async function refreshPendingTransactions() {
    const list = document.getElementById('pending-transactions-list');
    list.innerHTML = '<div style="text-align: center; padding: 20px;">Loading...</div>';
    
    try {
        // This endpoint needs to be implemented in Backend
        const response = await fetch('/active-transactions');
        const transactions = await response.json();
        
        if (Object.keys(transactions).length === 0) {
            list.innerHTML = '<div style="text-align: center; padding: 20px; color: #666;">No pending transactions found.</div>';
            return;
        }
        
        let html = '';
        for (const [txnId, data] of Object.entries(transactions)) {
            html += `
            <div class="transaction-card">
                <div class="transaction-header">
                    <code>${txnId}</code>
                    <span class="transaction-badge">${data.type || 'UNKNOWN'}</span>
                </div>
                <div class="transaction-details">
                    <span><strong>Status:</strong> ${data.status}</span>
                    <span><strong>Node:</strong> ${data.node || 'Unknown'}</span>
                </div>
                <div class="transaction-actions">
                    <button class="commit-action-btn" onclick="resolveTransaction('${txnId}', 'COMMIT')">✓ COMMIT</button>
                    <button class="rollback-action-btn" onclick="resolveTransaction('${txnId}', 'ROLLBACK')">✗ ROLLBACK</button>
                </div>
            </div>`;
        }
        list.innerHTML = html;
        
    } catch (error) {
        console.error("Error fetching transactions:", error);
        list.innerHTML = '<div style="text-align: center; padding: 20px; color: red;">Error loading transactions. Backend not ready?</div>';
    }
}

async function resolveTransaction(txnId, action) {
    if (!confirm(`Are you sure you want to ${action} this transaction?`)) return;
    
    try {
        const response = await fetch('/resolve-transaction', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ txnId, action })
        });
        
        const result = await response.json();
        alert(result.message);
        refreshPendingTransactions(); // Refresh list
        
    } catch (error) {
        alert("Error resolving transaction: " + error);
    }
}

// Load node status from backend
async function loadNodeStatus() {
    try {
        const response = await fetch('/status');
        const status = await response.json();
        
        console.log('Node status:', status);
        
        // Update each node card with real-time data
        for (const [nodeKey, nodeData] of Object.entries(status)) {
            updateNodeCard(nodeKey, nodeData);
        }
        
    } catch (error) {
        console.error('Error loading node status:', error);
        
        // Show error state for all nodes
        ['node1', 'node2', 'node3'].forEach(nodeKey => {
            updateNodeCard(nodeKey, {
                status: 'ERROR',
                rows: 0,
                lastUpdate: 'N/A'
            });
        });
    }
}

function updateNodeCard(nodeKey, nodeData) {
    const statusElement = document.getElementById(`${nodeKey}-status`);
    const rowsElement = document.getElementById(`${nodeKey}-rows`);
    const updateElement = document.getElementById(`${nodeKey}-update`);
    
    if (statusElement) {
        statusElement.textContent = nodeData.status;
        
        if (nodeData.status === 'ONLINE') {
            statusElement.className = 'status-indicator status-online';
        } else {
            statusElement.className = 'status-indicator status-offline';
        }
    }
    
    if (rowsElement) {
        rowsElement.textContent = nodeData.rows.toLocaleString();
    }
    
    if (updateElement) {
        updateElement.textContent = nodeData.lastUpdate;
    }
}

// TODO: Implement real-time monitoring
// - Auto-refresh node status every 5-10 seconds
// - Monitor node health metrics
// - Display connection status
// - Show transaction logs
// - Track replication lag
// - Alert on node failures

// Auto-refresh node status (optional)
function startStatusMonitoring() {
    // Initial load
    loadNodeStatus();
    
    //to enable auto-refresh every 10 seconds
    setInterval(loadNodeStatus, 10000);
}

// Start monitoring when page loads
document.addEventListener('DOMContentLoaded', function() {
    // Read the current node from the hidden config div
    const config = document.getElementById('app-config');
    const currentNodeKey = config.getAttribute('data-current-node'); // e.g., "node1"
    const currentNodeId = parseInt(config.getAttribute('data-current-node-id')); // e.g., 1
    
    // Auto-navigate to this node's view
    navigateToNode(currentNodeId);
    
    // Start status monitoring
    startStatusMonitoring();
});
