let currentOffset = 0;
let currentLimit = 100;
let totalRows = 0;
let currentFilters = {
    titleId: '',
    title: '',
    region: ''
};
// --- NEW GLOBAL VARIABLE ---
let currentEditingRegion = ''; 

async function loadNodeData(nodeNumber) {
    currentOffset = 0; // Reset offset when loading new node
    
    // Clear the table and wait for user input
    const tableBody = document.getElementById('table-body');
    tableBody.innerHTML = '<tr><td colspan="9" style="text-align: center; color: #666;"><i>Select a filter and click "Apply Filters" to load data from the distributed network.</i></td></tr>';
    
    // Reset counts
    document.getElementById('current-rows').textContent = '0';
    document.getElementById('total-rows').textContent = '-';
    
    // Hide load more button
    document.getElementById('load-more-btn').style.display = 'none';
}

async function fetchMovies() {
    try {
        // Build query parameters
        const activeNode = currentNode || 1;

        const params = new URLSearchParams({
            offset: currentOffset,
            limit: currentLimit,
            titleId: currentFilters.titleId,
            title: currentFilters.title,
            region: currentFilters.region,
            node: `node${activeNode}`
        });

        // Fetch movies from backend
        const response = await fetch(`/movies?${params}`);
        const result = await response.json();
        
        console.log('Loaded data:', result);
        
        // Update total rows count
        totalRows = result.total;
        updateRowCount();
        
        // Clear or append to table
        const tableBody = document.getElementById('table-body');
        if (currentOffset === 0) {
            tableBody.innerHTML = '';
        }
        
        // Check if data is empty
        if (!result.data || result.data.length === 0) {
            if (currentOffset === 0) {
                tableBody.innerHTML = '<tr><td colspan="9" style="text-align: center;">No data available</td></tr>';
            }
            // Hide load more button if no more data
            document.getElementById('load-more-btn').style.display = 'none';
            return;
        }
        
        // Show load more button if there's more data
        if (currentOffset + result.data.length < totalRows) {
            document.getElementById('load-more-btn').style.display = 'block';
        } else {
            document.getElementById('load-more-btn').style.display = 'none';
        }
        
        // Populate table with data
        // Populate table with data
        result.data.forEach(movie => {
            const row = document.createElement('tr');
            // FIX: Ensure quotes don't break HTML if data contains quotes
            const safeMovie = JSON.stringify(movie).replace(/"/g, '&quot;');
            
            row.innerHTML = `
                <td title="${movie.titleId || 'N/A'}">${movie.titleId || 'N/A'}</td>
                <td>${movie.ordering || 'N/A'}</td>
                <td title="${movie.title || 'N/A'}">${movie.title || 'N/A'}</td>
                <td>${movie.region || 'N/A'}</td>
                <td>${movie.language || 'N/A'}</td>
                <td title="${movie.types || 'N/A'}">${movie.types || 'N/A'}</td>
                <td title="${movie.attributes || 'N/A'}">${movie.attributes || 'N/A'}</td>
                <td>${movie.isOriginalTitle == 1 ? 'Yes' : 'No'}</td>
                <td>
                    <button onclick='editRow(${safeMovie})'>Edit</button>
                    <!-- OPTIONAL: Pass region to deleteRow too if you want to optimize delete -->
                    <button class="delete-button" onclick="deleteRow('${movie.titleId}', '${movie.region || ''}')">Delete</button>
                </td>
            `;
            tableBody.appendChild(row);
        });
        
    } catch (error) {
        console.error('Error loading data:', error);
        const tableBody = document.getElementById('table-body');
        tableBody.innerHTML = '<tr><td colspan="9" style="text-align: center; color: red;">Error loading data</td></tr>';
    }
}

async function toggleCrashMode() {
    try {
        const response = await fetch('/toggle-crash-mode', { method: 'POST' });
        const result = await response.json();
        
        if (result.status === 'ENABLED') {
            alert("⚠️ CRASH SIMULATION ENABLED ⚠️\nThe next INSERT operation will pause for 10 seconds.\nKill the server during this pause to simulate a crash!");
        } else {
            alert("Crash simulation DISABLED.");
        }
    } catch (error) {
        console.error("Error toggling crash mode:", error);
    }
}

function updateRowCount() {
    const currentRows = Math.min(currentOffset + currentLimit, totalRows);
    document.getElementById('current-rows').textContent = currentRows;
    document.getElementById('total-rows').textContent = totalRows;
}

function loadMoreRows() {
    currentOffset += currentLimit;
    fetchMovies();
}

function applyFilters() {
    // Get filter values
    currentFilters.titleId = document.getElementById('filter-titleid').value.trim();
    currentFilters.title = document.getElementById('filter-title').value.trim();
    currentFilters.region = document.getElementById('filter-region').value.trim();
    
    // TODO: Backend needs to implement filter handling in /movies endpoint
    console.log('Applying filters:', currentFilters);
    
    // Reset offset and fetch
    currentOffset = 0;
    fetchMovies();
}

function clearFilters() {
    // Clear filter inputs
    document.getElementById('filter-titleid').value = '';
    document.getElementById('filter-title').value = '';
    document.getElementById('filter-region').value = '';
    
    // Clear filter values
    currentFilters = {
        titleId: '',
        title: '',
        region: ''
    };
    
    // Reset and fetch
    currentOffset = 0;
    fetchMovies();
}

function openInsertModal() {
    document.getElementById('insert-modal').classList.add('active');
}

function closeInsertModal() {
    document.getElementById('insert-modal').classList.remove('active');
    
    // Clear all form fields
    document.getElementById('insert-titleid').value = '';
    document.getElementById('ordering').value = '';
    document.getElementById('title-name').value = '';
    document.getElementById('title-region').value = '';
    document.getElementById('language').value = '';
    document.getElementById('types').value = '';
    document.getElementById('attributes').value = '';
    document.getElementById('is-original').value = '1';
}

async function submitInsert() {
    // Get all form values
    const titleId = document.getElementById('insert-titleid').value.trim();
    const ordering = document.getElementById('ordering').value;
    const titleName = document.getElementById('title-name').value;
    const titleRegion = document.getElementById('title-region').value;
    const language = document.getElementById('language').value;
    const types = document.getElementById('types').value;
    const attributes = document.getElementById('attributes').value;
    const isOriginal = document.getElementById('is-original').value;

    // Validate required fields
    if (!titleId || !ordering || !titleName || !titleRegion || !language || !types) {
        alert('Please fill in all required fields (marked with *)');
        return;
    }

    // Prepare the Payload
    const payload = {
        titleId: titleId,
        ordering: parseInt(ordering),
        title: titleName,
        region: titleRegion,
        language: language,
        types: types,
        attributes: attributes || 'N/A',
        isOriginalTitle: parseInt(isOriginal)
    };

    console.log('Sending to backend:', payload);

    try {
        const response = await fetch('/insert', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        const result = await response.json();

        // Show Feedback
        if (result.logs && Array.isArray(result.logs)) {
            alert(`Insert Status:\n${result.logs.join('\n')}`);
        } else if (result.error) {
            alert(`Insert Failed:\n${result.error}\nDetails: ${result.details || ''}`);
        } else {
            alert(`Insert Status:\n${JSON.stringify(result)}`);
        }
        
        // Refresh Data
        closeInsertModal();
        currentOffset = 0;
        fetchMovies();

    } catch (error) {
        console.error("Insert failed:", error);
        alert("Failed to insert record. Check console for details.");
    }
}

function editRow(movie) {
    // Populate edit modal with current data (only editable fields)
    currentEditingRegion = movie.region; 
    document.getElementById('edit-titleid').value = movie.titleId;
    document.getElementById('display-titleid').value = movie.titleId;
    document.getElementById('edit-ordering').value = movie.ordering;
    document.getElementById('edit-title').value = movie.title;
    
    // Open modal
    document.getElementById('edit-modal').classList.add('active');
}

function closeEditModal() {
    document.getElementById('edit-modal').classList.remove('active');
    currentEditingRegion = '';
}

async function submitUpdate() {
    // Get form values - only title and ordering
    const titleId = document.getElementById('edit-titleid').value;
    const ordering = document.getElementById('edit-ordering').value;
    const title = document.getElementById('edit-title').value;
    const region = currentEditingRegion;     
    // Validate

    if (!ordering || !title) {
        alert('Please fill in all required fields');
        return;
    }

    const payload = {
        titleId: titleId,
        ordering: parseInt(ordering),
        title: title,
        region: region
    };

    console.log('Updating:', payload);

    try {
        const response = await fetch('/update', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        const result = await response.json();
        
        // Show Feedback
        if (result.logs && Array.isArray(result.logs)) {
            alert(`Update Status:\n${result.logs.join('\n')}`);
        } else if (result.error) {
            alert(`Update Failed:\n${result.error}\nDetails: ${result.details || ''}`);
        } else {
            alert(`Update Status:\n${JSON.stringify(result)}`);
        }
        
        // Refresh Data
        closeEditModal();
        currentOffset = 0;
        fetchMovies();

    } catch (error) {
        console.error("Update failed:", error);
        alert("Failed to update record. Check console for details.");
    }
}

async function deleteRow(titleId, region) {
    if (!confirm(`Are you sure you want to delete ${titleId}?`)) {
        return;
    }

    // If region wasn't passed, warn but continue
    if (!region) {
        console.warn('Region not provided for delete operation');
    }

    try {
        const response = await fetch('/delete', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 
                titleId: titleId,
                region: region  // ← ADD THIS
            })
        });

        const result = await response.json();
        
        // Show Feedback
        if (result.logs && Array.isArray(result.logs)) {
            alert(`Delete Status:\n${result.logs.join('\n')}`);
        } else if (result.error) {
            alert(`Delete Failed:\n${result.error}\nDetails: ${result.details || ''}`);
        } else {
            alert(`Delete Status:\n${JSON.stringify(result)}`);
        }
        
        // Refresh Data
        currentOffset = 0;
        fetchMovies();

    } catch (error) {
        console.error("Delete failed:", error);
        alert("Failed to delete record. Check console for details.");
    }
}

async function simulateConcurrency() {
    // TODO: Implement concurrency simulation in backend
    console.log('Simulate concurrency clicked');
    
    try {
        const response = await fetch('/simulate-concurrency', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({})
        });

        const result = await response.json();
        
        alert(`Concurrency Simulation:\n${result.message}`);
        
    } catch (error) {
        console.error("Concurrency simulation failed:", error);
        alert("Concurrency simulation feature coming soon!");
    }
}

async function generateReport(type) {
    const activeNode = currentNode || 1;

    // Determine endpoint based on report type
    const endpoint = type === 1 ? '/report/distribution' : '/report/types';
    
    const modal = document.getElementById('report-modal');
    const content = document.getElementById('report-content');
    const title = document.getElementById('report-title');
    
    if (!modal || !content) {
        console.error("Report Modal elements not found!");
        return;
    }

    // Set loading state
    content.innerHTML = '<div style="text-align:center; padding: 20px;">Generating report...</div>';
    title.textContent = type === 1 ? "Report: Regional Distribution" : "Report: Content Types";
    modal.classList.add('active');
    
    try {
        const response = await fetch(`${endpoint}?node=node${activeNode}`);
        const result = await response.json();
        
        if (result.error) {
            content.innerHTML = `<div class="error-message">Error: ${result.error}</div>`;
        } else {
            // Parse the text report into a nice table
            const tableHTML = parseReportToTable(result.report);
            content.innerHTML = tableHTML;
        }
        
    } catch (error) {
        content.innerHTML = `<div class="error-message">Network Error: ${error}</div>`;
    }
}

function parseReportToTable(reportText) {
    const lines = reportText.split('\n');
    let html = '<table class="report-table">';
    
    lines.forEach((line, index) => {
        // Skip separator lines (lines containing only dashes or equals)
        if (line.match(/^[-=]+$/) || line.trim() === '') return;
        
        // Check if it's a header or title
        if (line.startsWith('REPORT:')) {
            html += `<caption>${line}</caption>`;
            return;
        }

        const columns = line.split('|').map(col => col.trim());
        
        if (columns.length >= 2) {
            if (line.includes('REGION') || line.includes('TYPE')) {
                // Table Header
                html += '<thead><tr>';
                columns.forEach(col => html += `<th>${col}</th>`);
                html += '</tr></thead><tbody>';
            } else if (line.includes('TOTAL')) {
                // Total Row
                html += '<tr class="total-row">';
                columns.forEach(col => html += `<td><strong>${col}</strong></td>`);
                html += '</tr>';
            } else {
                // Data Row
                html += '<tr>';
                columns.forEach(col => html += `<td>${col}</td>`);
                html += '</tr>';
            }
        }
    });
    
    html += '</tbody></table>';
    return html;
}

// Close modals when clicking outside - wait for DOM to load
document.addEventListener('DOMContentLoaded', function() {
    const insertModal = document.getElementById('insert-modal');
    if (insertModal) {
        insertModal.addEventListener('click', function(e) {
            if (e.target === this) {
                closeInsertModal();
            }
        });
    }
    
    const editModal = document.getElementById('edit-modal');
    if (editModal) {
        editModal.addEventListener('click', function(e) {
            if (e.target === this) {
                closeEditModal();
            }
        });
    }

    const reportModal = document.getElementById('report-modal');
    if (reportModal) {
        reportModal.addEventListener('click', function(e) {
            if (e.target === this) {
                reportModal.classList.remove('active');
            }
        });
    }
});