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

function applySettings() {
    const isolationLevel = document.getElementById('isolation-level').value;
    const failureSimulation = document.getElementById('failure-simulation').value;
    
    console.log('Applying settings:', { isolationLevel, failureSimulation });
    
    // TODO: Send settings to backend
    // - Apply isolation level to transactions
    // - Configure failure simulation scenarios
    // - Update backend transaction handling
    
    alert(`Settings applied:\nIsolation Level: ${isolationLevel}\nFailure Simulation: ${failureSimulation}`);
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
