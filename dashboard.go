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
        .badge { padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 12px; display: inline-block; }
        .badge-processing { color: #087f5b; background: #e6fcf5; }
        .badge-completed { color: #495057; background: #e9ecef; }
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
                    <th>Progress</th>
                    <th>Success</th>
                    <th>Failed</th>
                    <th>Action</th>
                </tr>
            </thead>
            <tbody>
                {{range .}}
                <tr>
                    <td style="font-family: monospace;">{{.BatchID}}</td>
                    <td>
                        {{if ge .Done .Total}}
                            <span class="badge badge-completed">COMPLETED</span>
                        {{else}}
                            <span class="badge badge-processing">PROCESSING</span>
                        {{end}}
                    </td>
                    <td>{{.Done}} / {{.Total}}</td>
                    <td class="text-success">{{.Success}}</td>
                    <td class="text-failed">{{.Failed}}</td>
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
        .summary-box { display: flex; gap: 20px; background: #f8f9fa; padding: 15px; border-radius: 8px; margin-bottom: 20px; font-weight: 600; }
        .text-success { color: #2b8a3e; }
        .text-failed { color: #c92a2a; }
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
        </div>

        <h3 class="text-failed">Failed Items</h3>
        <input type="text" id="searchFailed" class="search-box" placeholder="Search failed emails..." onkeyup="resetPage('failed'); renderTable('failed')">
        <table id="tableFailed">
            <thead><tr><th>Email / Key</th><th>Message</th></tr></thead>
            <tbody>
                {{range .FailedItems}}
                <tr class="failed-row" data-email="{{.Key}}"><td class="email-col">{{.Key}}</td><td>{{.Message}}</td></tr>
                {{end}}
            </tbody>
        </table>
        <div class="pagination" id="pagFailed"></div>

        <h3 class="text-success" style="margin-top: 40px;">Success Items</h3>
        <input type="text" id="searchSuccess" class="search-box" placeholder="Search success emails..." onkeyup="resetPage('success'); renderTable('success')">
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

    <script>
        const rowsPerPage = 10;
        let pages = { success: 1, failed: 1 };

        function resetPage(type) { pages[type] = 1; }
        
        function changePage(type, newPage) {
            pages[type] = newPage;
            renderTable(type);
        }

        function renderTable(type) {
            const searchInput = document.getElementById(type === 'success' ? 'searchSuccess' : 'searchFailed').value.toLowerCase();
            const rows = Array.from(document.getElementsByClassName(type + '-row'));
            
            // Filter first
            const filteredRows = rows.filter(row => {
                const email = row.getAttribute('data-email').toLowerCase();
                const match = email.includes(searchInput);
                row.style.display = 'none'; // hide all initially
                return match;
            });

            // Calculate pagination
            const totalPages = Math.ceil(filteredRows.length / rowsPerPage) || 1;
            if (pages[type] > totalPages) pages[type] = totalPages;
            
            const start = (pages[type] - 1) * rowsPerPage;
            const end = start + rowsPerPage;

            // Show paginated rows
            filteredRows.slice(start, end).forEach(row => row.style.display = '');

            // Render controls
            const pagContainer = document.getElementById(type === 'success' ? 'pagSuccess' : 'pagFailed');
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
        };
    </script>
</body>
</html>
`
