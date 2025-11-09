// ========================================
// Socket.IO Dashboard - No REST API
// ========================================

const socket = io({
    transports: ['websocket', 'polling'],
    reconnection: true,
    reconnectionDelay: 1000,
    reconnectionAttempts: 10
});

// Handle connection
socket.on('connect', () => {
    console.log('✅ WebSocket connected');
    document.title = '🟢 ETL Monitor - Connected';
    // Request initial data
    updateDashboard();
});

// Handle disconnection
socket.on('disconnect', () => {
    console.log('❌ WebSocket disconnected');
    document.title = '🔴 ETL Monitor - Disconnected';
});

// Handle initial state
socket.on('initial_state', (state) => {
    console.log('📦 Received initial state');
    handleStatusUpdate(state);
});

// Handle state updates
socket.on('state_update', (data) => {
    console.log('🔄 State updated:', data.key);
    updateDashboard();
});

// Handle status updates
socket.on('status_update', (data) => {
    handleStatusUpdate(data);
});

// Handle tables updates
socket.on('tables_update', (tables) => {
    handleTablesUpdate(tables);
});

// Handle logs updates
socket.on('logs_update', (logs) => {
    handleLogsUpdate(logs);
});

// Request updates via socket
function updateDashboard() {
    socket.emit('request_status');
    socket.emit('request_tables');
    socket.emit('request_logs');
}

// Handle status data
function handleStatusUpdate(data) {
    // Update status badge
    const badge = document.getElementById('statusBadge');
    badge.textContent = data.status.toUpperCase();
    badge.className = 'status-badge status-' + data.status;
    
    // Update elapsed time
    document.getElementById('elapsedTime').textContent = data.elapsed_time || '00:00:00';
    
    // Update stats
    document.getElementById('totalTables').textContent = data.total_tables;
    document.getElementById('completedTables').textContent = data.completed_tables;
    document.getElementById('failedTables').textContent = data.failed_tables;
    
    const progress = data.total_tables > 0 
        ? Math.round((data.completed_tables / data.total_tables) * 100) 
        : 0;
    document.getElementById('progressPercent').textContent = progress + '%';
    
    // Update header stats
    const sys = data.system_stats || {cpu_percent: 0, memory_percent: 0};
    document.getElementById('headerCpu').textContent = sys.cpu_percent.toFixed(1) + '%';
    document.getElementById('headerMemory').textContent = sys.memory_percent.toFixed(1) + '%';
    
    // Update workers count
    document.getElementById('headerWorkers').textContent = data.max_workers || '20';
    
    // Update countdown if interval mode is enabled
    if (data.next_run_time && data.etl_interval > 0) {
        const now = Date.now() / 1000;
        const timeLeft = Math.max(0, data.next_run_time - now);
        
        if (timeLeft > 0) {
            const days = Math.floor(timeLeft / 86400);
            const hours = Math.floor((timeLeft % 86400) / 3600);
            const minutes = Math.floor((timeLeft % 3600) / 60);
            const seconds = Math.floor(timeLeft % 60);
            
            let countdownText = '';
            if (days > 0) countdownText += `${days}d `;
            if (hours > 0 || days > 0) countdownText += `${hours}h `;
            if (minutes > 0 || hours > 0 || days > 0) countdownText += `${minutes}m `;
            countdownText += `${seconds}s`;
            
            document.getElementById('countdownTime').textContent = countdownText;
            document.getElementById('nextRunCountdown').style.display = 'inline';
        } else {
            document.getElementById('nextRunCountdown').style.display = 'none';
        }
    } else {
        document.getElementById('nextRunCountdown').style.display = 'none';
    }
    
    // Update workers display and store data
    if (data.workers && Object.keys(data.workers).length > 0) {
        // Initialize workersData if needed
        if (!window.workersData) window.workersData = {};
        
        // Store ALL workers data (not just when div exists)
        Object.keys(data.workers).forEach(tableName => {
            const workers = data.workers[tableName];
            window.workersData[tableName] = { workers };
            
            console.log(`📊 Workers data stored for ${tableName}:`, workers);
        });
    } else {
        // Clear workers data if no workers
        window.workersData = {};
    }
}

// Handle tables data
function handleTablesUpdate(tables) {
    // Update progress section
    const progressDiv = document.getElementById('currentTableProgress');
    const processingTables = tables.filter(t => t.status === 'processing');
    
    if (processingTables.length === 0) {
        progressDiv.innerHTML = '<div style="text-align: center; padding: 20px; color: #999;">No table processing...</div>';
    } else {
        progressDiv.innerHTML = processingTables.map(table => {
            const percentage = table.progress || 0;
            const batchInfo = table.chunk_size && table.batch_size 
                ? `<div style="font-size: 10px; color: #666; margin-top: 3px;">
                     Chunk: ${table.chunk_size.toLocaleString()} | Batch: ${table.batch_size.toLocaleString()} | 
                     Producers: ${table.num_producers} | Consumers: ${table.num_consumers}
                   </div>`
                : '';
            
            return `
                <div class="table-progress-item">
                    <div class="table-progress-header">
                        <div class="table-progress-name">${table.name}</div>
                        <div class="table-progress-stats">
                            ${table.dst_rows.toLocaleString()} / ${table.src_rows.toLocaleString()} rows
                        </div>
                    </div>
                    <div class="progress-bar">
                        <div class="progress-fill" style="width: ${percentage}%;"></div>
                        <div class="progress-text">${percentage}%</div>
                    </div>
                    ${batchInfo}
                </div>
            `;
        }).join('');
    }
    
    // Update tables list
    const tableList = document.getElementById('tableList');
    
    if (tables.length === 0) {
        tableList.innerHTML = '<div style="text-align: center; padding: 40px; color: #999;">No tables found</div>';
        return;
    }
    
    tableList.innerHTML = tables.map(table => {
        const isMatch = table.src_rows === table.dst_rows && table.status === 'completed';
        const matchIcon = isMatch ? '[OK]' : (table.status === 'completed' ? '[!]' : (table.status === 'failed' ? '[X]' : '[ ]'));
        
        let statusClass = '';
        if (table.status === 'processing') statusClass = 'table-processing';
        if (table.status === 'retrying') statusClass = 'table-retrying';
        
        const batchInfo = table.chunk_size && table.batch_size
            ? `<div style="font-size: 10px; color: #999; margin-top: 2px;">
                 Chunk: ${table.chunk_size.toLocaleString()} | Batch: ${table.batch_size.toLocaleString()}
               </div>`
            : '';
        
        return `
            <div class="table-item ${table.status === 'processing' ? 'current-table' : ''}" 
                 style="${table.status === 'failed' ? 'background: #ffe0e0;' : ''}">
                <div style="flex: 1;">
                    <div class="table-name">${matchIcon} ${table.name}</div>
                    ${batchInfo}
                    ${table.error ? `<div class="error-msg">ERROR: ${table.error}</div>` : ''}
                </div>
                <div class="table-rows">
                    ${table.src_rows.toLocaleString()} / ${table.dst_rows.toLocaleString()}
                </div>
                <div class="table-status ${statusClass}">
                    ${table.status.toUpperCase()}
                </div>
            </div>
        `;
    }).join('');
}

// Handle logs data
function handleLogsUpdate(logs) {
    const logList = document.getElementById('logList');
    
    if (logs.length === 0) {
        logList.innerHTML = '<div style="text-align: center; padding: 20px; color: #999;">No logs yet...</div>';
        return;
    }
    
    // Show logs in reverse order (newest first)
    const reversedLogs = [...logs].reverse();
    
    logList.innerHTML = reversedLogs.map(log => {
        return `
            <div class="log-item">
                <span class="log-time">[${log.time}]</span>
                <span class="log-level log-level-${log.level}">${log.level}</span>
                <span>${log.message}</span>
            </div>
        `;
    }).join('');
    
    // Auto-scroll to top (newest)
    if (logList.scrollHeight > logList.clientHeight) {
        logList.scrollTop = 0;
    }
}

// Workers Modal Functions
function openWorkersModal() {
    const modal = document.getElementById('workersModal');
    modal.style.display = 'flex';
    
    console.log('🔍 Opening Workers Modal');
    console.log('Workers Data:', window.workersData);
    
    // Render immediately
    renderWorkersModal();
    
    // Auto-update every 5 seconds
    if (window.workersModalInterval) {
        clearInterval(window.workersModalInterval);
    }
    window.workersModalInterval = setInterval(() => {
        if (modal.style.display === 'flex') {
            console.log('🔄 Auto-updating Workers Modal...');
            renderWorkersModal();
        } else {
            clearInterval(window.workersModalInterval);
            window.workersModalInterval = null;
        }
    }, 5000);
}

function renderWorkersModal() {
    const content = document.getElementById('workersModalContent');
    
    console.log('📊 Rendering Workers Modal');
    console.log('Workers Data available:', window.workersData);
    
    if (!window.workersData || Object.keys(window.workersData).length === 0) {
        console.log('⚠️ No workers data available');
        content.innerHTML = '<div style="text-align: center; padding: 40px; color: #999;">No workers running...</div>';
        return;
    }
    
    console.log('✅ Workers data found, rendering...');
    
    let html = '';
    Object.keys(window.workersData).forEach(tableName => {
        const { workers } = window.workersData[tableName];
        
        html += `<div style="margin-bottom: 20px; padding: 15px; border: 1px solid #ddd;">`;
        html += `<h3 style="margin-bottom: 10px; font-size: 14px;">${tableName}</h3>`;
        
        Object.keys(workers).forEach(workerKey => {
            const worker = workers[workerKey];
            const type = worker.type || 'unknown';
            const rows = worker.rows_processed || worker.rows_loaded || 0;
            const total = worker.total_rows || 100;
            const percentage = total > 0 ? Math.round((rows / total) * 100) : 0;
            
            html += `
                <div class="worker-bar">
                    <div class="worker-bar-header">
                        <div class="worker-bar-label">${workerKey}</div>
                        <div class="worker-bar-value">${rows.toLocaleString()} / ${total.toLocaleString()} rows</div>
                    </div>
                    <div class="worker-bar-track">
                        <div class="worker-bar-fill ${type}" style="width: ${percentage}%">${percentage}%</div>
                    </div>
                </div>
            `;
        });
        
        html += `</div>`;
    });
    
    content.innerHTML = html;
}

// Settings Functions (via Socket.IO)
function openSettings() {
    const modal = document.getElementById('settingsModal');
    modal.style.display = 'flex';
    // Request config via socket
    socket.emit('request_config');
}

socket.on('config_update', (config) => {
    // Source Database
    document.getElementById('srcDbHost').value = config.src_db_host || '';
    document.getElementById('srcDbPort').value = config.src_db_port || 3306;
    
    // Check if using LATEST: pattern
    const srcDbName = config.src_db_name || '';
    if (srcDbName.startsWith('LATEST:')) {
        // Auto mode
        document.getElementById('srcDbNameMode').value = 'auto';
        document.getElementById('srcDbNameAuto').value = srcDbName.replace('LATEST:', '').trim();
        document.getElementById('srcDbNameManualGroup').style.display = 'none';
        document.getElementById('srcDbNameAutoGroup').style.display = 'block';
    } else {
        // Manual mode
        document.getElementById('srcDbNameMode').value = 'manual';
        document.getElementById('srcDbNameManual').value = srcDbName;
        document.getElementById('srcDbNameManualGroup').style.display = 'block';
        document.getElementById('srcDbNameAutoGroup').style.display = 'none';
    }
    
    document.getElementById('srcDbUser').value = config.src_db_user || '';
    document.getElementById('srcDbPassword').value = config.src_db_password || '';
    
    // Destination Database
    document.getElementById('dstDbHost').value = config.dst_db_host || '';
    document.getElementById('dstDbPort').value = config.dst_db_port || 3306;
    document.getElementById('dstDbName').value = config.dst_db_name || '';
    document.getElementById('dstDbDynamic').checked = config.dst_db_dynamic === 'true' || config.dst_db_dynamic === true;
    document.getElementById('dstDbUser').value = config.dst_db_user || '';
    document.getElementById('dstDbPassword').value = config.dst_db_password || '';
    
    // Table Filtering
    document.getElementById('includeTables').value = config.include_tables || '';
    document.getElementById('excludeTables').value = config.exclude_tables || '';
    
    // ETL Configuration
    document.getElementById('logLevel').value = config.log_level || 'INFO';
    document.getElementById('maxWorkersInput').value = config.max_workers || 20;
    document.getElementById('batchSizeInput').value = config.batch_size || 5000;
    
    // REDCap Integration
    document.getElementById('redcapEnabled').checked = config.redcap_enabled === 'true' || config.redcap_enabled === true;
    document.getElementById('redcapApiUrl').value = config.redcap_api_url || 'https://md.redcap.kku.ac.th/api/';
    document.getElementById('redcapApiToken').value = config.redcap_api_token || '';
    document.getElementById('redcapTableName').value = config.redcap_table_name || 'redcap_data';
    
    // Dynamic Performance Tuning
    document.getElementById('maxTableTime').value = config.max_table_time_seconds || 0;
    document.getElementById('minBatchSize').value = config.min_batch_size || 5000;
    document.getElementById('maxBatchSize').value = config.max_batch_size || 10000;
    
    // Convert ETL_INTERVAL_SECONDS to days/hours/minutes
    const intervalSeconds = config.etl_interval_seconds || 0;
    const days = Math.floor(intervalSeconds / 86400);
    const hours = Math.floor((intervalSeconds % 86400) / 3600);
    const minutes = Math.floor((intervalSeconds % 3600) / 60);
    
    document.getElementById('etlIntervalDays').value = days;
    document.getElementById('etlIntervalHours').value = hours;
    document.getElementById('etlIntervalMinutes').value = minutes;
});

function saveSettings() {
    // Build SRC_DB_NAME based on mode
    const srcDbNameMode = document.getElementById('srcDbNameMode').value;
    let srcDbName = '';
    
    if (srcDbNameMode === 'auto') {
        // Auto mode - prepend LATEST:
        const pattern = document.getElementById('srcDbNameAuto').value.trim();
        if (!pattern) {
            alert('กรุณาระบุ pattern สำหรับหา database ล่าสุด (เช่น smartmedkku_*)');
            return;
        }
        srcDbName = `LATEST:${pattern}`;
    } else {
        // Manual mode - use as-is
        srcDbName = document.getElementById('srcDbNameManual').value.trim();
        if (!srcDbName) {
            alert('กรุณาระบุชื่อ database');
            return;
        }
    }
    
    // Collect all settings
    const config = {
        // Source Database
        src_db_host: document.getElementById('srcDbHost').value,
        src_db_port: parseInt(document.getElementById('srcDbPort').value),
        src_db_name: srcDbName,
        src_db_user: document.getElementById('srcDbUser').value,
        src_db_password: document.getElementById('srcDbPassword').value,
        
        // Destination Database
        dst_db_host: document.getElementById('dstDbHost').value,
        dst_db_port: parseInt(document.getElementById('dstDbPort').value),
        dst_db_name: document.getElementById('dstDbName').value,
        dst_db_dynamic: document.getElementById('dstDbDynamic').checked ? 'true' : 'false',
        dst_db_user: document.getElementById('dstDbUser').value,
        dst_db_password: document.getElementById('dstDbPassword').value,
        
        // Table Filtering
        include_tables: document.getElementById('includeTables').value,
        exclude_tables: document.getElementById('excludeTables').value,
        
        // ETL Configuration
        log_level: document.getElementById('logLevel').value,
        max_workers: parseInt(document.getElementById('maxWorkersInput').value),
        batch_size: parseInt(document.getElementById('batchSizeInput').value),
        
        // REDCap Integration
        redcap_enabled: document.getElementById('redcapEnabled').checked ? 'true' : 'false',
        redcap_api_url: document.getElementById('redcapApiUrl').value,
        redcap_api_token: document.getElementById('redcapApiToken').value,
        redcap_table_name: document.getElementById('redcapTableName').value,
        
        // Dynamic Performance Tuning
        max_table_time_seconds: parseInt(document.getElementById('maxTableTime').value),
        min_batch_size: parseInt(document.getElementById('minBatchSize').value),
        max_batch_size: parseInt(document.getElementById('maxBatchSize').value),
        
        // Convert days/hours/minutes to seconds
        etl_interval_seconds: (
            (parseInt(document.getElementById('etlIntervalDays').value) || 0) * 86400 +
            (parseInt(document.getElementById('etlIntervalHours').value) || 0) * 3600 +
            (parseInt(document.getElementById('etlIntervalMinutes').value) || 0) * 60
        )
    };
    
    // Send via socket
    socket.emit('save_config', config);
}

socket.on('config_saved', (result) => {
    if (result.success) {
        alert('Settings saved! Changes will apply on next ETL run.');
        closeSettings();
    } else {
        alert('Error saving settings: ' + result.error);
    }
});

// Auto-refresh every 2 seconds
setInterval(updateDashboard, 2000);

// Initial load
updateDashboard();
