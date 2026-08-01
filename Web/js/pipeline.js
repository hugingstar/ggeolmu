// Web/js/pipeline.js
// 파이프라인 모니터링 관제 프론트엔드 JS 모듈

document.addEventListener("DOMContentLoaded", () => {
    const tbody = document.getElementById("pipeline-logs-tbody");
    const btnRefresh = document.getElementById("btn-refresh-pipeline");
    const statTotal = document.getElementById("stat-total-executions");
    const statSuccess = document.getElementById("stat-success-executions");
    const statFailed = document.getElementById("stat-failed-executions");
    const statLatest = document.getElementById("stat-latest-timestamp");

    const modal = document.getElementById("detail-modal");
    const modalTitle = document.getElementById("modal-title");
    const modalBody = document.getElementById("modal-body");
    const modalCloseBtn = document.getElementById("modal-close-btn");

    if (modalCloseBtn) {
        modalCloseBtn.addEventListener("click", () => {
            modal.classList.add("hidden");
        });
    }

    async function fetchPipelineLogs() {
        try {
            const res = await fetch("/api/pipeline/logs?limit=50");
            const data = await res.json();
            
            if (data.status === "success" && data.logs) {
                renderSummaryStats(data.logs);
                renderLogsTable(data.logs);
            } else {
                tbody.innerHTML = `<tr><td colspan="7" style="text-align: center; padding: 2rem; color: var(--text-muted);">파이프라인 로깅 기록이 없습니다.</td></tr>`;
            }
        } catch (err) {
            console.error("Pipeline logs fetch error:", err);
            tbody.innerHTML = `<tr><td colspan="7" style="text-align: center; padding: 2rem; color: #f87171;">데이터 로딩 중 오류 발생 (${err.message})</td></tr>`;
        }
    }

    function renderSummaryStats(logs) {
        const total = logs.length;
        let success = 0;
        let failed = 0;
        let latestTime = "-";

        logs.forEach(log => {
            if (log.status === "SUCCESS") success++;
            else if (log.status === "FAILED") failed++;
        });

        if (total > 0 && logs[0].end_time) {
            latestTime = logs[0].end_time;
        } else if (total > 0 && logs[0].start_time) {
            latestTime = logs[0].start_time;
        }

        if (statTotal) statTotal.textContent = `${total} 회`;
        if (statSuccess) statSuccess.textContent = `${success} 회`;
        if (statFailed) statFailed.textContent = `${failed} 회`;
        if (statLatest) statLatest.textContent = latestTime;
    }

    function renderLogsTable(logs) {
        if (!logs || logs.length === 0) {
            tbody.innerHTML = `<tr><td colspan="7" style="text-align: center; padding: 2rem; color: var(--text-muted);">파이프라인 로깅 기록이 없습니다.</td></tr>`;
            return;
        }

        tbody.innerHTML = logs.map(log => {
            let statusBadge = "";
            if (log.status === "SUCCESS") {
                statusBadge = `<span style="display: inline-block; padding: 0.2rem 0.6rem; border-radius: 9999px; background: rgba(52, 211, 153, 0.15); color: #34d399; font-weight: 600; font-size: 0.8rem;">🟢 SUCCESS</span>`;
            } else if (log.status === "FAILED") {
                statusBadge = `<span style="display: inline-block; padding: 0.2rem 0.6rem; border-radius: 9999px; background: rgba(248, 113, 113, 0.15); color: #f87171; font-weight: 600; font-size: 0.8rem;">🔴 FAILED</span>`;
            } else {
                statusBadge = `<span style="display: inline-block; padding: 0.2rem 0.6rem; border-radius: 9999px; background: rgba(96, 165, 250, 0.15); color: #60a5fa; font-weight: 600; font-size: 0.8rem;">🔵 RUNNING</span>`;
            }

            const durationStr = log.duration_seconds !== null ? `${log.duration_seconds} 초` : "-";
            const detailJson = JSON.stringify({
                execution_id: log.execution_id,
                market: log.market,
                start_time: log.start_time,
                end_time: log.end_time,
                duration_seconds: log.duration_seconds,
                status: log.status,
                step_details: _safeJsonParse(log.step_details),
                error_message: log.error_message
            }).replace(/'/g, "&apos;").replace(/"/g, "&quot;");

            return `
                <tr style="border-bottom: 1px solid rgba(255, 255, 255, 0.05); transition: background 0.2s;">
                    <td style="padding: 0.75rem; font-family: 'JetBrains Mono', monospace; font-size: 0.85rem; color: #94a3b8;">${log.execution_id}</td>
                    <td style="padding: 0.75rem; font-weight: 600; color: #f1f5f9;">${log.market}</td>
                    <td style="padding: 0.75rem; color: #cbd5e1;">${log.start_time || '-'}</td>
                    <td style="padding: 0.75rem; color: #cbd5e1;">${log.end_time || '-'}</td>
                    <td style="padding: 0.75rem; font-family: 'JetBrains Mono', monospace; color: #38bdf8;">${durationStr}</td>
                    <td style="padding: 0.75rem;">${statusBadge}</td>
                    <td style="padding: 0.75rem;">
                        <button class="tag-btn btn-view-detail" data-json="${detailJson}" style="padding: 0.3rem 0.6rem; font-size: 0.8rem;">🔍 상세 보기</button>
                    </td>
                </tr>
            `;
        }).join("");

        // 상세 보기 버튼 이벤트 리스너 바인딩
        document.querySelectorAll(".btn-view-detail").forEach(btn => {
            btn.addEventListener("click", (e) => {
                const jsonAttr = e.target.getAttribute("data-json");
                try {
                    const parsed = JSON.parse(jsonAttr);
                    modalTitle.textContent = `📋 [${parsed.market}] 세부 진행 내역 (${parsed.execution_id})`;
                    modalBody.textContent = JSON.stringify(parsed, null, 2);
                    modal.classList.remove("hidden");
                } catch (err) {
                    console.error("Modal json parse error:", err);
                }
            });
        });
    }

    function _safeJsonParse(input) {
        if (!input) return {};
        if (typeof input === 'object') return input;
        try {
            return JSON.parse(input);
        } catch {
            return input;
        }
    }

    if (btnRefresh) {
        btnRefresh.addEventListener("click", fetchPipelineLogs);
    }

    // 초기 로딩 및 5초 주기 자동 갱신
    fetchPipelineLogs();
    setInterval(fetchPipelineLogs, 5000);
});
