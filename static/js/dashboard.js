let currentNode = null;

// Determine the local node from the backend
async function detectLocalNode() {
    try {
        const response = await fetch('/status');
        const status = await response.json();
        
        // Check if backend provides local node information
        if (status.local_node_id) {
            currentNode = status.local_node_id;
        } else {
            // Default to Node 1 if not specified
            currentNode = 1;
        }
        
        // Update the page title
        const nodeTitle = currentNode === 1 ? 'Node 1 (Central)' : `Node ${currentNode} (Regional)`;
        document.getElementById('current-node-title').textContent = nodeTitle;
        
        console.log(`Local Node detected: ${currentNode}`);
        
        return currentNode;
        
    } catch (error) {
        console.error('Error detecting local node:', error);
        currentNode = 1; // Default to Node 1
        document.getElementById('current-node-title').textContent = 'Node 1 (Central)';
        return 1;
    }
}

function applySettings() {
    const isolationLevel = document.getElementById('isolation-level').value;
    const autoCommitToggle = document.getElementById('autocommit-toggle').checked;
    
    console.log('Applying settings:', { isolationLevel, autoCommit: autoCommitToggle });
    
    // Map frontend values to backend expected format
    const isolationLevelMap = {
        'read-uncommitted': 'READ UNCOMMITTED',
        'read-committed': 'READ COMMITTED',
        'repeatable-read': 'REPEATABLE READ',
        'serializable': 'SERIALIZABLE'
    };
    
    const settingsPayload = {
        isolationLevel: isolationLevelMap[isolationLevel],
        autoCommit: autoCommitToggle.toString()
    };
    
    // Send settings to backend
    fetch('/settings', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(settingsPayload),
    })
    .then(response => response.json())
    .then(data => {
        console.log('Backend response:', data);
        const mode = autoCommitToggle ? 'Auto Commit' : 'Manual 2PC';
        alert(`Settings applied successfully!\n\nIsolation Level: ${isolationLevelMap[isolationLevel]}\nTransaction Mode: ${mode}\n\n${data.logs ? data.logs.join('\n') : ''}`);
        
        // Update commit button state
        updateCommitButtonState();
    })
    .catch((error) => {
        console.error('Error sending settings:', error);
        alert('Failed to apply settings. Check console for details.');
    });
}

// Update the state of the Commit Changes button based on autocommit toggle
function updateCommitButtonState() {
    const autoCommitToggle = document.getElementById('autocommit-toggle');
    const commitButton = document.getElementById('commit-changes-btn');
    
    if (autoCommitToggle && commitButton) {
        if (autoCommitToggle.checked) {
            // Auto Commit ON: Disable button
            commitButton.disabled = true;
            commitButton.style.opacity = '0.5';
            commitButton.style.cursor = 'not-allowed';
            commitButton.title = 'Disabled in Auto Commit mode';
        } else {
            // Manual 2PC Mode: Enable button
            commitButton.disabled = false;
            commitButton.style.opacity = '1';
            commitButton.style.cursor = 'pointer';
            commitButton.title = 'View and resolve pending transactions';
        }
    }
}

// Load node status from backend
async function loadNodeStatus() {
    try {
        const response = await fetch('/status');
        const status = await response.json();
        
        console.log('Node status:', status);
        
        // Update each node status card
        ['node1', 'node2', 'node3'].forEach(nodeKey => {
            if (status[nodeKey]) {
                updateNodeStatus(nodeKey, status[nodeKey]);
            }
        });
        
    } catch (error) {
        console.error('Error loading node status:', error);
        
        // Show error state for all nodes
        ['node1', 'node2', 'node3'].forEach(nodeKey => {
            updateNodeStatus(nodeKey, {
                status: 'ERROR',
                rows: 0,
                lastUpdate: 'N/A'
            });
        });
    }
}

function updateNodeStatus(nodeKey, nodeData) {
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

// Auto-refresh node status
function startStatusMonitoring() {
    // Initial load
    loadNodeStatus();
    
    // Auto-refresh every 10 seconds
    setInterval(loadNodeStatus, 10000);
}

// Load current settings from backend and update UI
async function loadCurrentSettings() {
    try {
        const response = await fetch('/status');
        const status = await response.json();
        
        if (status.current_settings) {
            const settings = status.current_settings;
            
            // Update autocommit toggle
            const autoCommitToggle = document.getElementById('autocommit-toggle');
            if (autoCommitToggle && settings.auto_commit !== undefined) {
                autoCommitToggle.checked = settings.auto_commit;
            }
            
            // Update isolation level dropdown
            const isolationSelect = document.getElementById('isolation-level');
            if (isolationSelect && settings.isolation_level) {
                const levelMap = {
                    'READ UNCOMMITTED': 'read-uncommitted',
                    'READ COMMITTED': 'read-committed',
                    'REPEATABLE READ': 'repeatable-read',
                    'SERIALIZABLE': 'serializable'
                };
                isolationSelect.value = levelMap[settings.isolation_level] || 'read-committed';
            }
            
            // Update commit button state after loading settings
            updateCommitButtonState();
        }
    } catch (error) {
        console.error('Error loading current settings:', error);
    }
}

// Open Commit Modal and load pending transactions
async function openCommitModal() {
    const modal = document.getElementById('commit-modal');
    const listContainer = document.getElementById('pending-transactions-list');
    
    if (!modal || !listContainer) {
        console.error('Commit modal elements not found');
        return;
    }
    
    modal.classList.add('active');
    listContainer.innerHTML = '<div style="text-align: center; padding: 20px; color: #666;">Loading pending transactions...</div>';
    
    await refreshPendingTransactions();
}

function closeCommitModal() {
    const modal = document.getElementById('commit-modal');
    if (modal) {
        modal.classList.remove('active');
    }
}

// Refresh and display pending transactions
async function refreshPendingTransactions() {
    const listContainer = document.getElementById('pending-transactions-list');
    
    if (!listContainer) return;
    
    try {
        const response = await fetch('/active-transactions');
        const transactions = await response.json();
        
        console.log('Pending transactions:', transactions);
        
        if (Object.keys(transactions).length === 0) {
            listContainer.innerHTML = `
                <div style="text-align: center; padding: 30px; color: #28a745; background-color: #d4edda; border-radius: 4px;">
                    <strong>✓ No pending transactions</strong><br>
                    <small style="color: #155724;">All transactions have been resolved.</small>
                </div>
            `;
            return;
        }
        
        // Build transaction cards
        let html = '';
        for (const [txnId, txnData] of Object.entries(transactions)) {
            html += `
                <div class="transaction-card">
                    <div class="transaction-header">
                        <div>
                            <strong>Transaction ID:</strong> <code>${txnId}</code>
                        </div>
                        <span class="transaction-badge">${txnData.type || 'UNKNOWN'}</span>
                    </div>
                    <div class="transaction-details">
                        <div><strong>Status:</strong> ${txnData.status || 'PENDING'}</div>
                        <div><strong>Connections:</strong> ${txnData.connection_count || 0} node(s)</div>
                    </div>
                    <div class="transaction-actions">
                        <button class="commit-action-btn" onclick="resolveTransaction('${txnId}', 'COMMIT')">
                            ✓ COMMIT
                        </button>
                        <button class="rollback-action-btn" onclick="resolveTransaction('${txnId}', 'ROLLBACK')">
                            ✗ ROLLBACK
                        </button>
                    </div>
                </div>
            `;
        }
        
        listContainer.innerHTML = html;
        
    } catch (error) {
        console.error('Error loading pending transactions:', error);
        listContainer.innerHTML = `
            <div style="text-align: center; padding: 20px; color: #dc3545; background-color: #f8d7da; border-radius: 4px;">
                <strong>Error loading transactions</strong><br>
                <small>${error.message}</small>
            </div>
        `;
    }
}

// Resolve a transaction (COMMIT or ROLLBACK)
async function resolveTransaction(txnId, action) {
    if (!confirm(`Are you sure you want to ${action} transaction ${txnId}?`)) {
        return;
    }
    
    try {
        const response = await fetch('/resolve-transaction', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                txnId: txnId,
                action: action
            })
        });
        
        const result = await response.json();
        
        console.log('Resolution result:', result);
        
        // Show feedback
        const message = `${action} Result:\n\n${result.message}\n\nLogs:\n${result.logs.join('\n')}`;
        alert(message);
        
        // Refresh the list
        await refreshPendingTransactions();
        
        // Refresh the data table
        if (typeof fetchMovies === 'function') {
            fetchMovies();
        }
        
    } catch (error) {
        console.error(`Error resolving transaction:`, error);
        alert(`Failed to ${action} transaction. Check console for details.`);
    }
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', async function() {
    // Detect local node first
    await detectLocalNode();
    
    // Start status monitoring
    startStatusMonitoring();
    
    // Load initial data for the current node
    loadNodeData(currentNode);
    
    // Load current settings from backend
    await loadCurrentSettings();
    
    // Set up toggle event listener
    const autoCommitToggle = document.getElementById('autocommit-toggle');
    if (autoCommitToggle) {
        autoCommitToggle.addEventListener('change', updateCommitButtonState);
    }
    
    // Initial button state update
    updateCommitButtonState();
    
    // Add click outside listener for commit modal
    const commitModal = document.getElementById('commit-modal');
    if (commitModal) {
        commitModal.addEventListener('click', function(e) {
            if (e.target === this) {
                closeCommitModal();
            }
        });
    }
});