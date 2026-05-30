(function () {
    const bootstrap = window.AKITA_UI_BOOTSTRAP || {};
    const storageKeys = {
        apiKey: "akita-ui-api-key",
        autoRefresh: "akita-ui-auto-refresh",
        refreshMs: "akita-ui-refresh-ms",
        activeSection: "akita-ui-active-section",
    };

    const state = {
        apiKey: sessionStorage.getItem(storageKeys.apiKey) || "",
        autoRefresh: sessionStorage.getItem(storageKeys.autoRefresh) !== "false",
        refreshMs: Number(sessionStorage.getItem(storageKeys.refreshMs) || bootstrap.defaultRefreshMs || 5000),
        activeSection: sessionStorage.getItem(storageKeys.activeSection) || "overview",
        fetching: false,
        refreshTimer: null,
        latestSummary: null,
        latestConfig: null,
        latestTasks: null,
        latestLogs: null,
        latestLedger: null,
    };

    const elements = {};

    document.addEventListener("DOMContentLoaded", init);

    function init() {
        cacheElements();
        applyBootstrap();
        bindNavigation();
        bindToolbar();
        bindSubmenus();
        bindTaskForms();
        bindFilters();
        bindAuthModal();
        bindNodeControls();
        setActiveSection(state.activeSection);
        syncToolbarState();
        updateSecurityBadge();
        updateConnectionBadge("Shell ready", "neutral");
        refreshAll();
        scheduleRefresh();
    }

    function cacheElements() {
        elements.navButtons = Array.from(document.querySelectorAll(".nav-button"));
        elements.sectionJumpButtons = Array.from(document.querySelectorAll("[data-section-target]"));
        elements.scrollButtons = Array.from(document.querySelectorAll("[data-scroll-target]"));
        elements.views = Array.from(document.querySelectorAll(".view"));
        elements.taskForms = Array.from(document.querySelectorAll(".task-submit-form"));

        elements.sidebarClusterName = document.getElementById("sidebarClusterName");
        elements.sidebarNodeName = document.getElementById("sidebarNodeName");
        elements.securityBadge = document.getElementById("securityBadge");
        elements.securityCopy = document.getElementById("securityCopy");
        elements.heroTitle = document.getElementById("heroTitle");
        elements.heroSubtitle = document.getElementById("heroSubtitle");
        elements.heroNodeLine = document.getElementById("heroNodeLine");
        elements.heroStatusLine = document.getElementById("heroStatusLine");
        elements.connectionBadge = document.getElementById("connectionBadge");
        elements.autoRefreshToggle = document.getElementById("autoRefreshToggle");
        elements.refreshIntervalSelect = document.getElementById("refreshIntervalSelect");
        elements.refreshButton = document.getElementById("refreshButton");
        elements.lastUpdatedLabel = document.getElementById("lastUpdatedLabel");
        elements.heroMetricGrid = document.getElementById("heroMetricGrid");
        elements.healthGaugeGrid = document.getElementById("healthGaugeGrid");
        elements.taskDistribution = document.getElementById("taskDistribution");
        elements.capabilityCloud = document.getElementById("capabilityCloud");
        elements.recentActivityFeed = document.getElementById("recentActivityFeed");
        elements.clusterHighlights = document.getElementById("clusterHighlights");
        elements.clusterTableBody = document.getElementById("clusterTableBody");
        elements.taskStatusFilter = document.getElementById("taskStatusFilter");
        elements.taskLimitInput = document.getElementById("taskLimitInput");
        elements.taskTableBody = document.getElementById("taskTableBody");
        elements.logsLevelFilter = document.getElementById("logsLevelFilter");
        elements.logsLimitInput = document.getElementById("logsLimitInput");
        elements.logsFeed = document.getElementById("logsFeed");
        elements.ledgerEventTypeFilter = document.getElementById("ledgerEventTypeFilter");
        elements.ledgerLimitInput = document.getElementById("ledgerLimitInput");
        elements.ledgerFeed = document.getElementById("ledgerFeed");
        elements.configGroups = document.getElementById("configGroups");
        elements.authModal = document.getElementById("authModal");
        elements.openAuthButton = document.getElementById("openAuthButton");
        elements.topbarAuthButton = document.getElementById("topbarAuthButton");
        elements.closeAuthModalButton = document.getElementById("closeAuthModalButton");
        elements.cancelAuthButton = document.getElementById("cancelAuthButton");
        elements.clearApiKeyButton = document.getElementById("clearApiKeyButton");
        elements.saveApiKeyButton = document.getElementById("saveApiKeyButton");
        elements.apiKeyInput = document.getElementById("apiKeyInput");
        elements.authModalCopy = document.getElementById("authModalCopy");
        elements.toastStack = document.getElementById("toastStack");
        elements.shutdownButton = document.getElementById("shutdownButton");
    }

    function applyBootstrap() {
        elements.sidebarClusterName.textContent = bootstrap.clusterName || "Cluster pending";
        elements.sidebarNodeName.textContent = bootstrap.nodeName || "Node pending";
        elements.heroTitle.textContent = `${bootstrap.appName || "Akita Genesis Control Room"}`;
        elements.heroSubtitle.textContent = `Node ${bootstrap.nodeName || "unknown"} in cluster ${bootstrap.clusterName || "unknown"}.`;
        elements.apiKeyInput.value = state.apiKey;
    }

    function bindNavigation() {
        elements.navButtons.forEach((button) => {
            button.addEventListener("click", () => setActiveSection(button.dataset.section));
        });

        elements.sectionJumpButtons.forEach((button) => {
            button.addEventListener("click", () => setActiveSection(button.dataset.sectionTarget));
        });
    }

    function bindToolbar() {
        elements.refreshButton.addEventListener("click", () => refreshAll());
        elements.autoRefreshToggle.addEventListener("change", () => {
            state.autoRefresh = elements.autoRefreshToggle.checked;
            sessionStorage.setItem(storageKeys.autoRefresh, String(state.autoRefresh));
            scheduleRefresh();
        });
        elements.refreshIntervalSelect.addEventListener("change", () => {
            state.refreshMs = Number(elements.refreshIntervalSelect.value);
            sessionStorage.setItem(storageKeys.refreshMs, String(state.refreshMs));
            scheduleRefresh();
            refreshAll();
        });
    }

    function bindSubmenus() {
        elements.scrollButtons.forEach((button) => {
            button.addEventListener("click", () => {
                const target = document.getElementById(button.dataset.scrollTarget);
                if (target) {
                    target.scrollIntoView({ behavior: "smooth", block: "start" });
                }
            });
        });
    }

    function bindTaskForms() {
        elements.taskForms.forEach((form) => {
            form.addEventListener("submit", async (event) => {
                event.preventDefault();
                const payloadInput = form.querySelector("[name='taskPayload']");
                const priorityInput = form.querySelector("[name='taskPriority']");
                const rawPayload = payloadInput.value.trim();

                let parsedPayload;
                try {
                    parsedPayload = JSON.parse(rawPayload);
                } catch (error) {
                    showToast("Task payload must be valid JSON.", true);
                    payloadInput.focus();
                    return;
                }

                const priority = Number(priorityInput.value || 10);

                try {
                    updateConnectionBadge("Submitting task…", "neutral");
                    const response = await fetchJson("/tasks/submit", {
                        method: "POST",
                        body: {
                            task_data: parsedPayload,
                            priority,
                        },
                    });
                    showToast(`Task queued: ${response.task_id || "submitted"}`);
                    setActiveSection("tasks");
                    refreshAll();
                } catch (error) {
                    handleApiError(error, "Task submission failed.");
                }
            });
        });
    }

    function bindFilters() {
        [
            elements.taskStatusFilter,
            elements.taskLimitInput,
            elements.logsLevelFilter,
            elements.logsLimitInput,
            elements.ledgerEventTypeFilter,
            elements.ledgerLimitInput,
        ].forEach((element) => {
            element.addEventListener("change", () => refreshAll());
        });
    }

    function bindAuthModal() {
        [elements.openAuthButton, elements.topbarAuthButton].forEach((button) => {
            button.addEventListener("click", () => openAuthModal());
        });

        elements.closeAuthModalButton.addEventListener("click", closeAuthModal);
        elements.cancelAuthButton.addEventListener("click", closeAuthModal);

        elements.clearApiKeyButton.addEventListener("click", () => {
            state.apiKey = "";
            sessionStorage.removeItem(storageKeys.apiKey);
            elements.apiKeyInput.value = "";
            updateSecurityBadge();
            closeAuthModal();
            refreshAll();
        });

        elements.saveApiKeyButton.addEventListener("click", () => {
            state.apiKey = elements.apiKeyInput.value.trim();
            if (state.apiKey) {
                sessionStorage.setItem(storageKeys.apiKey, state.apiKey);
                showToast("API key saved for this browser session.");
            } else {
                sessionStorage.removeItem(storageKeys.apiKey);
                showToast("Saved API key cleared.");
            }
            updateSecurityBadge();
            closeAuthModal();
            refreshAll();
        });

        elements.authModal.addEventListener("click", (event) => {
            if (event.target === elements.authModal) {
                closeAuthModal();
            }
        });
    }

    function bindNodeControls() {
        elements.shutdownButton.addEventListener("click", async () => {
            const confirmed = window.confirm("Request a graceful shutdown for this node?");
            if (!confirmed) {
                return;
            }

            try {
                await fetchJson("/shutdown", { method: "POST" });
                showToast("Shutdown request accepted by node.");
            } catch (error) {
                handleApiError(error, "Shutdown request failed.");
            }
        });
    }

    function setActiveSection(sectionName) {
        state.activeSection = sectionName;
        sessionStorage.setItem(storageKeys.activeSection, sectionName);

        elements.navButtons.forEach((button) => {
            button.classList.toggle("is-active", button.dataset.section === sectionName);
        });

        elements.views.forEach((view) => {
            view.classList.toggle("is-active", view.dataset.section === sectionName);
        });

        if (sectionName === "configuration" && !state.latestConfig) {
            refreshAll();
        }
    }

    function syncToolbarState() {
        elements.autoRefreshToggle.checked = state.autoRefresh;
        elements.refreshIntervalSelect.value = String(state.refreshMs);
    }

    function scheduleRefresh() {
        if (state.refreshTimer) {
            window.clearInterval(state.refreshTimer);
            state.refreshTimer = null;
        }

        if (state.autoRefresh) {
            state.refreshTimer = window.setInterval(refreshAll, state.refreshMs);
        }
    }

    async function refreshAll() {
        if (state.fetching) {
            return;
        }

        state.fetching = true;
        updateConnectionBadge("Refreshing telemetry…", "neutral");

        try {
            const [summary, tasks, logs, ledger, config] = await Promise.all([
                fetchJson("/dashboard/summary"),
                fetchTasks(),
                fetchLogs(),
                fetchLedger(),
                state.latestConfig ? Promise.resolve(state.latestConfig) : fetchConfig(),
            ]);

            state.latestSummary = summary;
            state.latestTasks = tasks;
            state.latestLogs = logs;
            state.latestLedger = ledger;
            state.latestConfig = config;

            renderSummary(summary);
            renderTasks(tasks);
            renderLogs(logs);
            renderLedger(ledger);
            renderConfig(config);

            const updatedTime = summary.generated_at || Date.now() / 1000;
            elements.lastUpdatedLabel.textContent = `Updated ${formatTimestamp(updatedTime)}`;
            updateConnectionBadge(`Live · ${formatTimestamp(updatedTime, true)}`, "ready");
        } catch (error) {
            handleApiError(error, "Unable to refresh node telemetry.");
        } finally {
            state.fetching = false;
        }
    }

    async function fetchTasks() {
        const params = new URLSearchParams();
        const status = elements.taskStatusFilter.value;
        const limit = clampNumber(elements.taskLimitInput.value, 5, 100, 12);
        params.set("limit", String(limit));
        if (status && status !== "all") {
            params.set("status", status);
        }
        return fetchJson(`/tasks?${params.toString()}`);
    }

    async function fetchLogs() {
        const params = new URLSearchParams();
        const level = elements.logsLevelFilter.value;
        const limit = clampNumber(elements.logsLimitInput.value, 20, 500, 120);
        params.set("limit", String(limit));
        if (level && level !== "all") {
            params.set("level", level);
        }
        return fetchJson(`/logs?${params.toString()}`);
    }

    async function fetchLedger() {
        const params = new URLSearchParams();
        const eventType = elements.ledgerEventTypeFilter.value.trim();
        const limit = clampNumber(elements.ledgerLimitInput.value, 5, 100, 25);
        params.set("limit", String(limit));
        if (eventType) {
            params.set("event_type", eventType);
        }
        return fetchJson(`/ledger?${params.toString()}`);
    }

    async function fetchConfig() {
        return fetchJson("/config");
    }

    async function fetchJson(path, options = {}) {
        const requestOptions = {
            method: options.method || "GET",
            headers: {
                Accept: "application/json",
            },
        };

        if (state.apiKey) {
            requestOptions.headers[bootstrap.apiKeyHeaderName || "X-API-Key"] = state.apiKey;
        }

        if (options.body) {
            requestOptions.headers["Content-Type"] = "application/json";
            requestOptions.body = JSON.stringify(options.body);
        }

        const response = await window.fetch(path, requestOptions);
        if (!response.ok) {
            const error = new Error(await describeErrorResponse(response));
            error.status = response.status;
            throw error;
        }
        return response.json();
    }

    async function describeErrorResponse(response) {
        try {
            const payload = await response.json();
            return payload.detail || payload.message || `${response.status} ${response.statusText}`;
        } catch (_error) {
            return `${response.status} ${response.statusText}`;
        }
    }

    function renderSummary(summary) {
        const cluster = summary.cluster || {};
        const node = summary.node || {};
        const tasks = summary.tasks || {};
        const security = summary.security || {};
        const counts = tasks.counts || {};
        const pendingCount = getCount(counts, "pending") + getCount(counts, "accepted");

        elements.sidebarClusterName.textContent = cluster.cluster_name || bootstrap.clusterName || "Unknown cluster";
        elements.sidebarNodeName.textContent = node.node_name || bootstrap.nodeName || "Unknown node";
        elements.heroNodeLine.textContent = `${node.node_name || "Node"} · ${node.status || "unknown"} · ${node.node_id || "pending"}`;
        elements.heroStatusLine.textContent = `${node.is_leader ? "Leader" : "Follower"} in ${cluster.cluster_name || "cluster"} with ${cluster.total_nodes_known || 0} known nodes and ${pendingCount} queued tasks.`;
        elements.heroTitle.textContent = `${bootstrap.appName || "Akita Genesis Control Room"}`;
        elements.heroSubtitle.textContent = `Version ${bootstrap.appVersion || "unknown"} · API ${security.api_secured ? "secured" : "open"} · Header ${security.api_key_header_name || bootstrap.apiKeyHeaderName || "X-API-Key"}`;

        const heroMetrics = [
            { label: "Node Status", value: toTitleCase(node.status || "unknown"), caption: node.is_leader ? "Leader role active" : "Follower role active" },
            { label: "Online Nodes", value: String(cluster.online_nodes_count || 0), caption: `${cluster.total_nodes_known || 0} known members` },
            { label: "Workers Ready", value: String(cluster.available_worker_count || 0), caption: "Schedulable workers now" },
            { label: "Queue Pressure", value: String(pendingCount), caption: "Pending + accepted tasks" },
            { label: "Uptime", value: formatDuration(node.uptime_seconds || 0), caption: `Leader ${node.current_leader_id || "unknown"}` },
            { label: "Security", value: security.api_secured ? "Secured" : "Open", caption: security.api_secured ? `${security.configured_api_key_count || 0} keys configured` : "No API key required" },
        ];

        elements.heroMetricGrid.innerHTML = heroMetrics.map((metric) => `
            <article class="hero-metric">
                <p>${escapeHtml(metric.label)}</p>
                <strong>${escapeHtml(metric.value)}</strong>
                <p>${escapeHtml(metric.caption)}</p>
            </article>
        `).join("");

        renderHealthGauges(node.resources || {}, node.current_task_count || 0);
        renderTaskDistribution(counts);
        renderCapabilityCloud(cluster.capability_counts || {});
        renderRecentActivity(summary.events || []);
        renderClusterHighlights(summary);
        updateSecurityBadge(summary.security || {});
    }

    function renderHealthGauges(resources, currentTaskCount) {
        const cpuPercent = Number(resources?.cpu?.percent_used || 0);
        const memoryPercent = Number(resources?.memory?.virtual?.percent_used || 0);
        const diskPercent = Number(resources?.disk?.root?.percent_used || 0);
        const networkDown = Number(resources?.network?.bytes_recv_rate_bps || 0);

        const gauges = [
            {
                title: "CPU",
                value: cpuPercent,
                accent: "var(--teal)",
                detail: `${resources?.cpu?.logical_cores || "?"} logical cores · ${formatNumber(resources?.cpu?.current_frequency_mhz, "MHz")}`,
            },
            {
                title: "Memory",
                value: memoryPercent,
                accent: "var(--amber)",
                detail: `${formatNumber(resources?.memory?.virtual?.used_gb, "GB")} used of ${formatNumber(resources?.memory?.virtual?.total_gb, "GB")}`,
            },
            {
                title: "Disk",
                value: diskPercent,
                accent: "var(--rose)",
                detail: `${formatNumber(resources?.disk?.root?.free_gb, "GB")} free on root volume`,
            },
            {
                title: "Load",
                value: Math.min(currentTaskCount * 10, 100),
                accent: "var(--navy-soft)",
                detail: `${currentTaskCount || 0} active tasks · ${formatBits(networkDown)} inbound`,
            },
        ];

        elements.healthGaugeGrid.innerHTML = gauges.map((gauge) => `
            <article class="gauge-card">
                <div class="gauge-ring" style="--gauge-value: ${Math.max(0, Math.min(gauge.value, 100))}%; --gauge-accent: ${gauge.accent};">
                    <span>${Math.round(Math.max(0, Math.min(gauge.value, 100)))}%</span>
                </div>
                <div class="gauge-details">
                    <strong>${escapeHtml(gauge.title)}</strong>
                    <p>${escapeHtml(gauge.detail)}</p>
                </div>
            </article>
        `).join("");
    }

    function renderTaskDistribution(counts) {
        const orderedStatuses = [
            "pending",
            "accepted",
            "assigned",
            "worker_ack",
            "processing",
            "completed",
            "failed",
            "timeout",
            "cancelled",
        ];
        const maximum = Math.max(1, ...orderedStatuses.map((status) => getCount(counts, status)));

        elements.taskDistribution.innerHTML = orderedStatuses.map((status) => {
            const value = getCount(counts, status);
            const width = (value / maximum) * 100;
            return `
                <article class="metric-bar">
                    <header>
                        <span>${escapeHtml(toTitleCase(status.replace("_", " ")))}</span>
                        <strong>${value}</strong>
                    </header>
                    <div class="metric-track"><span class="metric-fill" style="width: ${width}%;"></span></div>
                </article>
            `;
        }).join("");
    }

    function renderCapabilityCloud(capabilityCounts) {
        const entries = Object.entries(capabilityCounts);
        if (!entries.length) {
            elements.capabilityCloud.innerHTML = `<div class="empty-state">No capability telemetry reported yet.</div>`;
            return;
        }

        elements.capabilityCloud.innerHTML = entries.map(([capability, count]) => `
            <span class="tag">${escapeHtml(capability)} · ${count}</span>
        `).join("");
    }

    function renderRecentActivity(events) {
        if (!events.length) {
            elements.recentActivityFeed.innerHTML = `<div class="empty-state">No ledger events available yet.</div>`;
            return;
        }

        elements.recentActivityFeed.innerHTML = events.map((event) => `
            <article class="activity-card">
                <header>
                    <span class="status-badge is-neutral">${escapeHtml(event.event_type || "UNKNOWN")}</span>
                    <span>${escapeHtml(formatTimestamp(event.timestamp, true))}</span>
                </header>
                <p>${escapeHtml(describeEvent(event))}</p>
            </article>
        `).join("");
    }

    function renderClusterHighlights(summary) {
        const cluster = summary.cluster || {};
        const node = summary.node || {};
        const statusCounts = cluster.status_counts || {};
        const busiestNode = (cluster.busiest_nodes || [])[0];
        const cards = [
            {
                label: "Leader",
                value: cluster.current_leader_id || "Unassigned",
                caption: node.is_leader ? "This node currently owns leader duties." : "Current leader seen by this node.",
            },
            {
                label: "Healthy Members",
                value: String((statusCounts.online || 0) + (statusCounts.degraded || 0)),
                caption: `${statusCounts.offline || 0} offline · ${statusCounts.unknown || 0} unknown`,
            },
            {
                label: "Busiest Node",
                value: busiestNode ? busiestNode.node_name : "No load yet",
                caption: busiestNode ? `${busiestNode.current_task_count} tasks` : "No queue pressure recorded",
            },
        ];

        elements.clusterHighlights.innerHTML = cards.map((card) => `
            <article class="summary-card">
                <p>${escapeHtml(card.label)}</p>
                <strong>${escapeHtml(card.value)}</strong>
                <p>${escapeHtml(card.caption)}</p>
            </article>
        `).join("");

        renderClusterTable(cluster.nodes || []);
    }

    function renderClusterTable(nodes) {
        if (!nodes.length) {
            elements.clusterTableBody.innerHTML = `<tr><td colspan="7"><div class="empty-state">No cluster members available.</div></td></tr>`;
            return;
        }

        elements.clusterTableBody.innerHTML = nodes.map((node) => {
            const status = String(node.status || "unknown");
            const resources = node.resources || {};
            const cpu = resources?.cpu?.percent_used;
            const memory = resources?.memory?.virtual?.percent_used;
            const resourceText = [
                cpu !== undefined ? `CPU ${Math.round(cpu)}%` : null,
                memory !== undefined ? `RAM ${Math.round(memory)}%` : null,
            ].filter(Boolean).join(" · ") || "Awaiting metrics";

            return `
                <tr>
                    <td>
                        <strong>${escapeHtml(node.node_name || node.node_id)}</strong>
                        <div class="helper-copy mono">${escapeHtml(node.node_id || "unknown")}</div>
                    </td>
                    <td>${renderStatusBadge(status)}</td>
                    <td>${node.is_leader ? "Leader" : "Worker"}</td>
                    <td>${node.current_task_count || 0}</td>
                    <td>${(node.capabilities || []).length ? (node.capabilities || []).map((capability) => `<span class="tag">${escapeHtml(capability)}</span>`).join(" ") : '<span class="helper-copy">None</span>'}</td>
                    <td><div class="resource-stack"><span>${escapeHtml(resourceText)}</span><span class="helper-copy mono">${escapeHtml(node.address_hex || "No RNS address")}</span></div></td>
                    <td>${escapeHtml(formatTimestamp(node.last_seen, true))}</td>
                </tr>
            `;
        }).join("");
    }

    function renderTasks(payload) {
        const tasks = payload.tasks || [];
        if (!tasks.length) {
            elements.taskTableBody.innerHTML = `<tr><td colspan="6"><div class="empty-state">No tasks match the current filter.</div></td></tr>`;
            return;
        }

        elements.taskTableBody.innerHTML = tasks.map((task) => `
            <tr>
                <td><code>${escapeHtml(task.id || "unknown")}</code></td>
                <td>${renderStatusBadge(task.status || "unknown")}</td>
                <td>${task.priority ?? "-"}</td>
                <td>${escapeHtml(task.assigned_to_node_id || "Unassigned")}</td>
                <td>${escapeHtml(formatTimestamp(task.last_updated, true))}</td>
                <td>
                    <div class="payload-preview">
                        <details>
                            <summary>Inspect payload</summary>
                            <pre>${escapeHtml(JSON.stringify(task.data || {}, null, 2))}</pre>
                        </details>
                    </div>
                </td>
            </tr>
        `).join("");
    }

    function renderLogs(payload) {
        const logs = payload.logs || [];
        if (!logs.length) {
            elements.logsFeed.innerHTML = `<div class="empty-state">No logs available for the current filter.</div>`;
            return;
        }

        elements.logsFeed.innerHTML = logs.map((entry) => {
            const level = String(entry.level || "INFO").toUpperCase();
            return `
                <article class="log-entry">
                    <header>
                        ${renderStatusBadge(level.toLowerCase())}
                        <span>${escapeHtml(formatTimestamp(entry.timestamp, true))}</span>
                    </header>
                    <p class="helper-copy mono">${escapeHtml(entry.logger || "akita_genesis")}</p>
                    <p class="log-message mono">${escapeHtml(entry.message || "")}</p>
                </article>
            `;
        }).join("");
    }

    function renderLedger(payload) {
        const events = payload.events || [];
        if (!events.length) {
            elements.ledgerFeed.innerHTML = `<div class="empty-state">No ledger entries available for the current filter.</div>`;
            return;
        }

        elements.ledgerFeed.innerHTML = events.map((event) => `
            <article class="timeline-card">
                <header>
                    <span class="status-badge is-neutral">${escapeHtml(event.event_type || "UNKNOWN")}</span>
                    <span>${escapeHtml(formatTimestamp(event.timestamp, true))}</span>
                </header>
                <p>${escapeHtml(event.source_node_name || event.source_node_id || "Unknown source")}</p>
                <details>
                    <summary>Event details</summary>
                    <pre>${escapeHtml(JSON.stringify(event.details || {}, null, 2))}</pre>
                </details>
            </article>
        `).join("");
    }

    function renderConfig(payload) {
        if (!payload) {
            elements.configGroups.innerHTML = `<div class="empty-state">Configuration will load after the first successful refresh.</div>`;
            return;
        }

        elements.configGroups.innerHTML = Object.entries(payload).map(([groupName, values]) => `
            <article class="config-card">
                <p class="panel-eyebrow">${escapeHtml(groupName.replace(/_/g, " "))}</p>
                <div class="config-values">
                    ${Object.entries(values).map(([key, value]) => `
                        <div class="config-row">
                            <strong>${escapeHtml(key.replace(/_/g, " "))}</strong>
                            <p class="config-value mono">${escapeHtml(formatConfigValue(value))}</p>
                        </div>
                    `).join("")}
                </div>
            </article>
        `).join("");
    }

    function updateSecurityBadge(security = null) {
        const effectiveSecurity = security || {
            api_secured: Boolean(bootstrap.apiSecured),
            configured_api_key_count: bootstrap.configuredApiKeyCount || 0,
            api_key_header_name: bootstrap.apiKeyHeaderName || "X-API-Key",
        };

        if (effectiveSecurity.api_secured) {
            if (state.apiKey) {
                elements.securityBadge.textContent = `Unlocked · ${effectiveSecurity.api_key_header_name}`;
                elements.securityCopy.textContent = `${effectiveSecurity.configured_api_key_count || 0} key(s) configured on the node. Browser session key is loaded.`;
            } else {
                elements.securityBadge.textContent = `Secured · ${effectiveSecurity.api_key_header_name}`;
                elements.securityCopy.textContent = `This node expects an API key. Live telemetry will unlock after you provide one.`;
            }
        } else {
            elements.securityBadge.textContent = "Open API";
            elements.securityCopy.textContent = "This node does not require an API key for control API access.";
        }
    }

    function updateConnectionBadge(text, mode) {
        elements.connectionBadge.textContent = text;
        elements.connectionBadge.classList.toggle("is-error", mode === "error");
    }

    function openAuthModal(message) {
        elements.authModalCopy.textContent = message || "If the node control API is secured, enter a valid key to unlock live telemetry and control actions.";
        elements.apiKeyInput.value = state.apiKey;
        elements.authModal.classList.remove("hidden");
        elements.apiKeyInput.focus();
    }

    function closeAuthModal() {
        elements.authModal.classList.add("hidden");
    }

    function handleApiError(error, fallbackMessage) {
        const message = error && error.message ? error.message : fallbackMessage;
        updateConnectionBadge(message, "error");
        showToast(message || fallbackMessage || "Unexpected API error.", true);
        if (error && error.status === 403) {
            openAuthModal("The node rejected the request. Provide a valid API key to continue.");
        }
    }

    function showToast(message, isError = false) {
        const toast = document.createElement("div");
        toast.className = `toast${isError ? " is-error" : ""}`;
        toast.textContent = message;
        elements.toastStack.appendChild(toast);
        window.setTimeout(() => {
            toast.remove();
        }, 3800);
    }

    function renderStatusBadge(statusValue) {
        const normalized = String(statusValue || "unknown").toLowerCase();
        const statusClass = normalized === "online"
            ? "is-online"
            : normalized === "degraded" || normalized === "warning"
                ? "is-degraded"
                : normalized === "error" || normalized === "failed" || normalized === "offline"
                    ? "is-error"
                    : "is-neutral";
        return `<span class="status-badge ${statusClass}">${escapeHtml(toTitleCase(normalized.replace(/_/g, " ")))}</span>`;
    }

    function describeEvent(event) {
        const details = event.details || {};
        if (details.task_id) {
            return `${event.event_type} · task ${details.task_id}`;
        }
        if (details.reason) {
            return `${event.event_type} · ${details.reason}`;
        }
        return JSON.stringify(details) === "{}"
            ? `${event.event_type} from ${event.source_node_id || "unknown source"}`
            : JSON.stringify(details);
    }

    function getCount(counts, key) {
        return Number(counts[key] || 0);
    }

    function clampNumber(value, min, max, fallback) {
        const parsed = Number(value);
        if (Number.isNaN(parsed)) {
            return fallback;
        }
        return Math.max(min, Math.min(max, parsed));
    }

    function formatDuration(totalSeconds) {
        const seconds = Math.max(0, Math.floor(Number(totalSeconds) || 0));
        const hours = Math.floor(seconds / 3600);
        const minutes = Math.floor((seconds % 3600) / 60);
        const remainder = seconds % 60;
        if (hours > 0) {
            return `${hours}h ${minutes}m`;
        }
        if (minutes > 0) {
            return `${minutes}m ${remainder}s`;
        }
        return `${remainder}s`;
    }

    function formatTimestamp(timestamp, short = false) {
        if (!timestamp) {
            return "Unknown";
        }
        const date = new Date(Number(timestamp) * 1000);
        if (Number.isNaN(date.getTime())) {
            return String(timestamp);
        }
        return short
            ? date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })
            : date.toLocaleString();
    }

    function formatNumber(value, suffix) {
        if (value === undefined || value === null || value === "") {
            return `n/a${suffix ? ` ${suffix}` : ""}`;
        }
        return `${Number(value).toFixed(1)}${suffix ? ` ${suffix}` : ""}`;
    }

    function formatBits(value) {
        const numeric = Number(value || 0);
        if (numeric <= 0) {
            return "0 bps";
        }
        if (numeric >= 1_000_000_000) {
            return `${(numeric / 1_000_000_000).toFixed(2)} Gbps`;
        }
        if (numeric >= 1_000_000) {
            return `${(numeric / 1_000_000).toFixed(2)} Mbps`;
        }
        if (numeric >= 1_000) {
            return `${(numeric / 1_000).toFixed(2)} Kbps`;
        }
        return `${numeric.toFixed(0)} bps`;
    }

    function formatConfigValue(value) {
        if (Array.isArray(value)) {
            return value.join(", ") || "None";
        }
        if (value && typeof value === "object") {
            return JSON.stringify(value);
        }
        return value === null || value === undefined || value === "" ? "None" : String(value);
    }

    function toTitleCase(value) {
        return String(value || "")
            .split(" ")
            .map((chunk) => chunk ? chunk.charAt(0).toUpperCase() + chunk.slice(1) : chunk)
            .join(" ");
    }

    function escapeHtml(value) {
        return String(value)
            .replaceAll("&", "&amp;")
            .replaceAll("<", "&lt;")
            .replaceAll(">", "&gt;")
            .replaceAll('"', "&quot;")
            .replaceAll("'", "&#39;");
    }
})();