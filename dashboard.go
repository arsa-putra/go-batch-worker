package worker

// ==========================================
// HTML TEMPLATES
// ==========================================

const dashboardListHTML = `
<!DOCTYPE html>
<html>
<head>
    <title>Worker Dashboard</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif; margin: 0; padding: 40px; background: #f4f6f8; color: #333; }
        .container { max-width: 1000px; margin: auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 4px 12px rgba(0,0,0,0.05); }
        h2 { margin-top: 0; color: #111; font-size: 24px; border-bottom: 2px solid #f0f2f5; padding-bottom: 12px; }
        table { width: 100%; border-collapse: collapse; margin-top: 20px; }
        th, td { border: 1px solid #e9ecef; padding: 12px; text-align: left; font-size: 14px; }
        th { background-color: #f8f9fa; color: #495057; font-weight: 600; }
        .badge { padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 12px; display: inline-block; text-transform: capitalize; }
        
        .status-running { color: #004085; background: #cce5ff; }
        .status-completed { color: #155724; background: #d4edda; }
        
        .text-success { color: #2b8a3e; font-weight: bold; }
        .text-failed { color: #c92a2a; font-weight: bold; }
        .btn-view { background-color: #4c6ef5; color: white; text-decoration: none; padding: 6px 12px; border-radius: 4px; font-size: 13px; font-weight: 600; transition: 0.2s; }
        .btn-view:hover { background-color: #3b5bdb; }
    </style>
</head>
<body>
    <div class="container">
        <h2>Worker Batches Dashboard</h2>
        {{if .}}
        <table>
            <thead>
                <tr>
                    <th>Batch ID</th>
                    <th>Status</th>
                    <th>Submitted At</th>
                    <th>Total Data</th>
                    <th>Success</th>
                    <th>Failed</th>
                    <th>Processing</th>
                    <th>Action</th>
                </tr>
            </thead>
            <tbody>
                {{range .}}
                <tr>
                    <td style="font-family: monospace;">{{.BatchID}}</td>
                    <td>
                        <span class="badge status-{{ .Status }}">{{ .Status }}</span>
                    </td>
                    
                    <!-- Store Unix timestamp in data attribute for client-side formatting -->
                    <td class="date-cell" data-timestamp="{{ .CreatedAt }}">
                        {{ .CreatedAt }}
                    </td>
                    
                    <td>{{ .Total }}</td>
                    <td style="color: green;">{{ .Success }}</td>
                    <td style="color: red;">{{ .Failed }}</td>
                    <td style="color: orange;"><strong>{{ .Processing }}</strong></td>
                    <td>
                        <a href="batches/{{.BatchID}}/detail" class="btn-view" target="_blank">View Detail</a>
                    </td>
                </tr>
                {{end}}
            </tbody>
        </table>
        {{else}}
        <p>No active or historical batches found.</p>
        {{end}}
    </div>

    <script>
        // Format timestamp into readable local date format
        const dateCells = document.querySelectorAll('.date-cell');
        
        dateCells.forEach(cell => {
            const timestamp = parseInt(cell.getAttribute('data-timestamp')) * 1000;
            if (timestamp > 0) {
                const date = new Date(timestamp);
                cell.innerText = date.toLocaleString('id-ID'); 
            } else {
                cell.innerText = "-";
            }
        });
    </script>
</body>
</html>
`

const dashboardDetailHTML = `
<!DOCTYPE html>
<html>
<head>
    <title>Batch Detail - {{.BatchID}}</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif; margin: 0; padding: 40px; background: #f4f6f8; color: #333; }
        .container { max-width: 1200px; margin: auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 4px 12px rgba(0,0,0,0.05); }
        .summary-box { display: flex; gap: 20px; align-items: center; background: #f8f9fa; padding: 15px; border-radius: 8px; margin-bottom: 20px; font-weight: 600; }
        .text-success { color: #2b8a3e; }
        .text-failed { color: #c92a2a; }
        .text-processing { color: #f59f00; }
        .badge { padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 12px; display: inline-block; text-transform: capitalize; }
        .status-running { color: #004085; background: #cce5ff; }
        .status-completed { color: #155724; background: #d4edda; }
        
        /* Tab navigation styles */
        .tab-headers { display: flex; gap: 10px; border-bottom: 2px solid #e9ecef; margin-bottom: 20px; }
        .tab-btn { padding: 10px 20px; border: none; background: none; font-size: 15px; font-weight: 600; cursor: pointer; color: #495057; border-bottom: 3px solid transparent; margin-bottom: -2px; transition: 0.2s; }
        .tab-btn:hover { color: #4c6ef5; }
        .tab-btn.active { color: #4c6ef5; border-bottom-color: #4c6ef5; }
        .tab-content { display: none; }
        .tab-content.active { display: block; }

        table { width: 100%; border-collapse: collapse; margin-top: 15px; }
        th, td { border: 1px solid #e9ecef; padding: 10px; text-align: left; font-size: 14px; }
        th { background-color: #f1f3f5; }
        .search-box { margin-bottom: 10px; padding: 8px; width: 300px; border: 1px solid #ced4da; border-radius: 4px; }
        .pagination { margin-top: 15px; display: flex; gap: 5px; align-items: center; }
        .pagination button { padding: 6px 12px; border: 1px solid #ced4da; background: white; cursor: pointer; border-radius: 4px; }
        .pagination button:disabled { background: #f1f3f5; cursor: not-allowed; }
        .pagination button:hover:not(:disabled) { background: #e9ecef; }
        .page-info { font-size: 14px; margin: 0 10px; }
    </style>
</head>
<body>
    <div class="container">
        <h2>Batch Detail: <span style="font-family: monospace; font-size: 18px; color: #495057;">{{.BatchID}}</span></h2>
        
        <div class="summary-box">
            <div>Total: {{.Total}}</div>
            <div class="text-success">Success: {{.Success}}</div>
            <div class="text-failed">Failed: {{.Failed}}</div>
            <div class="text-processing">Processing: {{.Processing}}</div>
            <div style="margin-left: auto;">
                Status: <span class="badge status-{{.Status}}">{{.Status}}</span>
            </div>
        </div>

        <!-- Tab Navigation Headers (Success -> Failed -> Processing) -->
        <div class="tab-headers">
            <button class="tab-btn active" onclick="switchTab('success', this)">Success Items ({{.Success}})</button>
            <button class="tab-btn" onclick="switchTab('failed', this)">Failed Items ({{.Failed}})</button>
            <button class="tab-btn" onclick="switchTab('processing', this)">Processing / Waiting ({{.Processing}})</button>
        </div>

        <!-- Tab Content: Success Items -->
        <div id="tab-success" class="tab-content active">
            <input type="text" id="searchSuccess" class="search-box" placeholder="Search success keys..." onkeyup="resetPage('success'); renderTable('success')">
            <table id="tableSuccess">
                <thead><tr><th>Email / Key</th><th>Message</th></tr></thead>
                <tbody>
                    {{range .SuccessItems}}
                    <tr class="success-row" data-email="{{.Key}}"><td class="email-col">{{.Key}}</td><td>{{.Message}}</td></tr>
                    {{end}}
                </tbody>
            </table>
            <div class="pagination" id="pagSuccess"></div>
        </div>

        <!-- Tab Content: Failed Items -->
        <div id="tab-failed" class="tab-content">
            <input type="text" id="searchFailed" class="search-box" placeholder="Search failed keys..." onkeyup="resetPage('failed'); renderTable('failed')">
            <table id="tableFailed">
                <thead><tr><th>Email / Key</th><th>Message</th></tr></thead>
                <tbody>
                    {{range .FailedItems}}
                    <tr class="failed-row" data-email="{{.Key}}"><td class="email-col">{{.Key}}</td><td>{{.Message}}</td></tr>
                    {{end}}
                </tbody>
            </table>
            <div class="pagination" id="pagFailed"></div>
        </div>

        <!-- Tab Content: Processing / Waiting Items -->
        <div id="tab-processing" class="tab-content">
            <input type="text" id="searchProcessing" class="search-box" placeholder="Search processing keys..." onkeyup="resetPage('processing'); renderTable('processing')">
            <table id="tableProcessing">
                <thead>
                <tr>
                    <th>Email / Key</th>
                    <th>Payload Details</th>
                </tr>
                </thead>
                <tbody>
                    {{range .ProcessingItems}}
                    <tr class="processing-row" data-email="{{.}}">
                        <td style="font-family: monospace; font-weight: bold; width: 30%;">{{.email}}</td>
                        <td>
                            <code style="font-size: 12px; color: #495057; background: #f8f9fa; padding: 4px 8px; border-radius: 4px; display: block; max-height: 60px; overflow-y: auto;">
                                {{printf "%v" .}}
                            </code>
                        </td>
                    </tr>
                    {{else}}
                    <tr>
                        <td style="color: #adb5bd; font-style: italic;" colspan="1">No items currently processing.</td>
                    </tr>
                    {{end}}
                </tbody>
            </table>
            <div class="pagination" id="pagProcessing"></div>
        </div>
    </div>

    <script>
        const rowsPerPage = 10;
        let pages = { success: 1, failed: 1, processing: 1 };

        function switchTab(tabName, btnElement) {
            // Hide all tab contents
            document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
            // Remove active class from all tab buttons
            document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
            
            // Activate selected tab and button
            document.getElementById('tab-' + tabName).classList.add('active');
            btnElement.classList.add('active');
            
            // Render table for the active tab
            renderTable(tabName);
        }

        function resetPage(type) { pages[type] = 1; }
        
        function changePage(type, newPage) {
            pages[type] = newPage;
            renderTable(type);
        }

        function renderTable(type) {
            const searchInputEl = document.getElementById(type === 'success' ? 'searchSuccess' : (type === 'failed' ? 'searchFailed' : 'searchProcessing'));
            const searchInput = searchInputEl ? searchInputEl.value.toLowerCase() : '';
            const rows = Array.from(document.getElementsByClassName(type + '-row'));
            
            // Filter rows based on search input
            const filteredRows = rows.filter(row => {
                const email = row.getAttribute('data-email').toLowerCase();
                const match = email.includes(searchInput);
                row.style.display = 'none';
                return match;
            });

            // Calculate pagination limits
            const totalPages = Math.ceil(filteredRows.length / rowsPerPage) || 1;
            if (pages[type] > totalPages) pages[type] = totalPages;
            
            const start = (pages[type] - 1) * rowsPerPage;
            const end = start + rowsPerPage;

            // Display paginated slice
            filteredRows.slice(start, end).forEach(row => row.style.display = '');

            // Render pagination controls
            const pagContainer = document.getElementById(type === 'success' ? 'pagSuccess' : (type === 'failed' ? 'pagFailed' : 'pagProcessing'));
            if (!pagContainer) return;

            let prevDisabled = pages[type] === 1 ? 'disabled' : '';
            let nextDisabled = pages[type] === totalPages ? 'disabled' : '';

            pagContainer.innerHTML = 
                '<button onclick="changePage(\'' + type + '\', ' + (pages[type] - 1) + ')" ' + prevDisabled + '>Prev</button>' +
                '<span class="page-info">Page ' + pages[type] + ' of ' + totalPages + ' (' + filteredRows.length + ' items)</span>' +
                '<button onclick="changePage(\'' + type + '\', ' + (pages[type] + 1) + ')" ' + nextDisabled + '>Next</button>';
        }

        window.onload = function() {
            renderTable('success');
            renderTable('failed');
            renderTable('processing');
        };
    </script>
</body>
</html>
`
