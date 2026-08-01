// Views containing HTML rendering and initialization logic for each page
import { escapeHtml } from './router.js';

const API_BASE = '/api';

let autocompleteTimeout = null;

function formatNumber(num) {
  if (num === null || num === undefined) return '-';
  return Number(num).toLocaleString();
}

function formatChange(val) {
  if (val === null || val === undefined) return '-';
  const pct = (val * 100).toFixed(2);
  return pct > 0 ? `+${pct}%` : `${pct}%`;
}

function renderSvgChart(history) {
  if (!history || history.length === 0) {
    return `<div class="no-chart-data">차트 데이터를 불러올 수 없습니다.</div>`;
  }

  const closes = history.map(h => h.close).filter(c => c !== null);
  if (closes.length === 0) {
    return `<div class="no-chart-data">가격 데이터가 부족합니다.</div>`;
  }

  const minVal = Math.min(...closes) * 0.995;
  const maxVal = Math.max(...closes) * 1.005;
  const range = maxVal - minVal || 1;

  const width = 500;
  const height = 180;
  const padding = 25;

  const chartWidth = width - padding * 2;
  const chartHeight = height - padding * 2;

  const points = history.map((h, i) => {
    if (h.close === null) return null;
    const x = padding + (i / (history.length - 1)) * chartWidth;
    const y = padding + chartHeight - ((h.close - minVal) / range) * chartHeight;
    return { x, y, val: h.close, date: h.date, vol: h.volume };
  }).filter(p => p !== null);

  const linePath = points.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x} ${p.y}`).join(' ');
  const areaPath = `${linePath} L ${points[points.length - 1].x} ${height - padding} L ${points[0].x} ${height - padding} Z`;

  const gridLines = [];
  for (let i = 0; i <= 3; i++) {
    const y = padding + (i / 3) * chartHeight;
    const price = maxVal - (i / 3) * range;
    gridLines.push(`
      <line x1="${padding}" y1="${y}" x2="${width - padding}" y2="${y}" stroke="rgba(255,255,255,0.05)" stroke-dasharray="2 2" />
      <text x="${width - padding + 5}" y="${y + 4}" fill="rgba(255,255,255,0.4)" font-size="8">${formatNumber(Math.round(price))}</text>
    `);
  }

  const dateLabels = points.map(p => `
    <text x="${p.x}" y="${height - 5}" fill="rgba(255,255,255,0.5)" font-size="9" text-anchor="middle">${p.date.substring(5)}</text>
    <circle cx="${p.x}" cy="${p.y}" r="4" fill="#3b82f6" stroke="#f8fafc" stroke-width="1.5" class="chart-point" />
  `).join('');

  return `
    <svg viewBox="0 0 ${width} ${height}" class="stock-svg-chart">
      <defs>
        <linearGradient id="chartGrad" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="#3b82f6" stop-opacity="0.4"/>
          <stop offset="100%" stop-color="#3b82f6" stop-opacity="0.0"/>
        </linearGradient>
      </defs>
      ${gridLines.join('')}
      <path d="${areaPath}" fill="url(#chartGrad)" />
      <path d="${linePath}" fill="none" stroke="#3b82f6" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" style="filter: drop-shadow(0px 2px 8px rgba(59, 130, 246, 0.5));" />
      ${dateLabels}
    </svg>
  `;
}

export const dashboardView = {
  path: '/',
  view: async () => {
    let stats = { total_rows: 0, unique_stocks: 0, total_logs: 0, latest_date: '-' };
    try {
      const res = await fetch(`${API_BASE}/stats`);
      if (res.ok) stats = await res.json();
    } catch (e) {
      console.error('Failed to load stats', e);
    }

    const searchHistory = JSON.parse(localStorage.getItem('searchHistory') || '[]');
    const historyHtml = searchHistory.length > 0
      ? searchHistory.map(symbol => `
          <a href="/stock/${symbol}" data-link class="badge-history">${escapeHtml(symbol)}</a>
        `).join('')
      : '<span class="text-muted small">최근 검색 기록이 없습니다.</span>';

    return `
      <div class="glass-container main-wrapper">
        <header>
          <h1>🤖 Ggeolmu 4-Tier Stock Search</h1>
          <p>독립 4-Tier 아키텍처(WEB-WAS-DB-Manager) 기반 주식 분석 및 Multi-Agent 프롬프트 대시보드</p>
        </header>

        <section class="stats-grid">
          <div class="stat-card">
            <span class="stat-title">수집된 데이터 건수</span>
            <span class="stat-value text-gradient-blue">${formatNumber(stats.total_rows)}</span>
          </div>
          <div class="stat-card">
            <span class="stat-title">전체 등록 종목</span>
            <span class="stat-value text-gradient-green">${formatNumber(stats.unique_stocks)}</span>
          </div>
          <div class="stat-card">
            <span class="stat-title">에이전트 검사 횟수</span>
            <span class="stat-value text-gradient-purple">${formatNumber(stats.total_logs)}</span>
          </div>
          <div class="stat-card">
            <span class="stat-title">최근 데이터 갱신일</span>
            <span class="stat-value text-muted">${escapeHtml(stats.latest_date || '-')}</span>
          </div>
        </section>

        <main>
          <section class="search-section">
            <div class="input-group search-container">
              <input type="text" id="symbol-input" placeholder="종목 코드 또는 종목명 입력 (예: 005930, AAPL)" autocomplete="off">
              <button id="search-btn">조회 및 프롬프트 생성</button>
              <div id="autocomplete-box" class="autocomplete-dropdown hidden"></div>
            </div>
            
            <div class="history-panel">
              <span class="history-label">최근 검색:</span>
              <div class="history-list">${historyHtml}</div>
            </div>
          </section>

          <section class="quick-links-section">
            <h3>📈 추천 인기 종목</h3>
            <div class="quick-grid">
              <a href="/stock/005930" data-link class="quick-card">
                <span class="quick-name">삼성전자</span>
                <span class="quick-symbol">005930</span>
              </a>
              <a href="/stock/000660" data-link class="quick-card">
                <span class="quick-name">SK하이닉스</span>
                <span class="quick-symbol">000660</span>
              </a>
              <a href="/stock/AAPL" data-link class="quick-card">
                <span class="quick-name">Apple Inc.</span>
                <span class="quick-symbol">AAPL</span>
              </a>
              <a href="/stock/NVDA" data-link class="quick-card">
                <span class="quick-name">NVIDIA Corp.</span>
                <span class="quick-symbol">NVDA</span>
              </a>
            </div>
          </section>
        </main>
      </div>
    `;
  },
  init: async () => {
    const searchBtn = document.getElementById('search-btn');
    const symbolInput = document.getElementById('symbol-input');
    const autocompleteBox = document.getElementById('autocomplete-box');

    const doSearch = () => {
      const q = symbolInput.value.trim().toUpperCase();
      if (!q) return;
      
      let history = JSON.parse(localStorage.getItem('searchHistory') || '[]');
      history = [q, ...history.filter(item => item !== q)].slice(0, 5);
      localStorage.setItem('searchHistory', JSON.stringify(history));

      window.routerInstance.navigateTo(`/stock/${q}`);
    };

    if (searchBtn) searchBtn.addEventListener('click', doSearch);
    if (symbolInput) {
      symbolInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') doSearch();
      });

      symbolInput.addEventListener('input', (e) => {
        const val = e.target.value.trim();
        if (autocompleteTimeout) clearTimeout(autocompleteTimeout);
        if (val.length < 2) {
          autocompleteBox.classList.add('hidden');
          return;
        }

        autocompleteTimeout = setTimeout(async () => {
          try {
            const res = await fetch(`${API_BASE}/search?q=${encodeURIComponent(val)}`);
            if (res.ok) {
              const list = await res.json();
              if (list.length === 0) {
                autocompleteBox.classList.add('hidden');
                return;
              }
              autocompleteBox.innerHTML = list.map(item => `
                <div class="autocomplete-item" data-symbol="${escapeHtml(item.symbol)}">
                  <span class="ac-name">${escapeHtml(item.name)}</span>
                  <span class="ac-symbol">${escapeHtml(item.symbol)}</span>
                </div>
              `).join('');
              autocompleteBox.classList.remove('hidden');
            }
          } catch (err) {
            console.error('Autocomplete failed', err);
          }
        }, 200);
      });
    }

    if (autocompleteBox) {
      autocompleteBox.addEventListener('click', (e) => {
        const item = e.target.closest('.autocomplete-item');
        if (item) {
          const sym = item.getAttribute('data-symbol');
          symbolInput.value = sym;
          autocompleteBox.classList.add('hidden');
          doSearch();
        }
      });
    }
  }
};

export const stockDetailView = {
  path: /^\/stock\/([a-zA-Z0-9_-]+)$/,
  view: async (params) => {
    const symbol = params.symbol;

    return `
      <div class="glass-container main-wrapper">
        <header class="detail-header">
          <div class="header-left">
            <a href="/" data-link class="back-link">◀ 검색으로 이동</a>
            <h1>종목 분석: <span class="highlight-symbol">${escapeHtml(symbol)}</span></h1>
          </div>
        </header>

        <div id="detail-content-area">
          <div class="loader">종목 데이터 및 에이전트 검사 수행 중...</div>
        </div>
      </div>
    `;
  },
  init: async (params) => {
    const symbol = params.symbol;
    const container = document.getElementById('detail-content-area');

    try {
      const [promptRes, detailRes] = await Promise.all([
        fetch(`${API_BASE}/prompt?symbol=${encodeURIComponent(symbol)}`),
        fetch(`${API_BASE}/stock/${encodeURIComponent(symbol)}`)
      ]);

      let promptData = null;
      let stockDetail = null;

      if (promptRes.ok) promptData = await promptRes.json();
      if (detailRes.ok) stockDetail = await detailRes.json();

      let auditStatusBadge = '';
      if (promptData) {
        if (promptData.is_valid) {
          auditStatusBadge = `<span class="badge badge-success">🛡️ 검토 통과 (안전)</span>`;
        } else {
          auditStatusBadge = `<span class="badge badge-danger">⚠️ 검토 거부 (${escapeHtml(promptData.audit_reason)})</span>`;
        }
      }

      let historyTableRows = '';
      if (stockDetail && stockDetail.history) {
        historyTableRows = stockDetail.history.map(h => `
          <tr>
            <td>${escapeHtml(h.date)}</td>
            <td>${formatNumber(h.close)}</td>
            <td class="${h.change > 0 ? 'text-green' : h.change < 0 ? 'text-red' : ''}">${formatChange(h.change)}</td>
            <td>${formatNumber(h.volume)}</td>
          </tr>
        `).join('');
      }

      container.innerHTML = `
        <div class="detail-grid">
          <div class="detail-card left-card">
            <div class="card-header">
              <h2>${escapeHtml(stockDetail ? stockDetail.name : symbol)} <span class="sym-sub">(${escapeHtml(symbol)})</span></h2>
              ${auditStatusBadge}
            </div>

            <div class="chart-wrapper">
              <h3>📈 최근 가격 트렌드 (5일)</h3>
              ${renderSvgChart(stockDetail ? stockDetail.history : [])}
            </div>

            <div class="table-wrapper">
              <table class="data-table">
                <thead>
                  <tr>
                    <th>일자</th>
                    <th>종가</th>
                    <th>등락률</th>
                    <th>거래량</th>
                  </tr>
                </thead>
                <tbody>
                  ${historyTableRows || '<tr><td colspan="4" class="text-center text-muted">데이터가 없습니다.</td></tr>'}
                </tbody>
              </table>
            </div>
          </div>

          <div class="detail-card right-card">
            <div class="card-header">
              <h2>✍️ Multi-Agent 생성 프롬프트</h2>
              <button id="copy-prompt-btn" class="secondary-btn">복사하기</button>
            </div>

            <p class="text-muted small">아래 프롬프트를 복사하여 ChatGPT 또는 Gemini에 입력하면 퀀트 트레이더 관점의 맞춤 분석을 받으실 수 있습니다.</p>

            <textarea id="prompt-output" readonly>${escapeHtml(promptData ? promptData.generated_prompt : '프롬프트를 불러올 수 없습니다.')}</textarea>
          </div>
        </div>
      `;

      const copyBtn = document.getElementById('copy-prompt-btn');
      const promptTextarea = document.getElementById('prompt-output');
      if (copyBtn && promptTextarea) {
        copyBtn.addEventListener('click', () => {
          promptTextarea.select();
          navigator.clipboard.writeText(promptTextarea.value);
          copyBtn.innerText = '복사 완료! ✅';
          setTimeout(() => { copyBtn.innerText = '복사하기'; }, 2000);
        });
      }

    } catch (e) {
      console.error('Failed to load stock details', e);
      container.innerHTML = `
        <div class="error-panel">
          <h3>❌ 종목 데이터 조회 실패</h3>
          <p class="text-muted">${escapeHtml(e.message)}</p>
        </div>
      `;
    }
  }
};

export const logsView = {
  path: '/logs',
  view: async () => {
    let logs = [];
    try {
      const res = await fetch(`${API_BASE}/logs?limit=50`);
      if (res.ok) logs = await res.json();
    } catch (e) {
      console.error('Failed to fetch logs', e);
    }

    const logRows = logs.map(l => `
      <tr>
        <td>${l.id}</td>
        <td><a href="/stock/${l.symbol}" data-link class="log-symbol-link">${escapeHtml(l.symbol)}</a></td>
        <td>
          <span class="badge ${l.status === 'PASS' ? 'badge-success' : 'badge-danger'}">
            ${l.status}
          </span>
        </td>
        <td>${escapeHtml(l.created_at ? l.created_at.substring(0, 19).replace('T', ' ') : '-')}</td>
        <td>
          <button class="secondary-btn-sm view-prompt-btn" data-prompt="${escapeHtml(l.generated_prompt || '')}">프롬프트 보기</button>
        </td>
      </tr>
    `).join('');

    return `
      <div class="glass-container main-wrapper">
        <header class="detail-header">
          <div class="header-left">
            <a href="/" data-link class="back-link">◀ 홈으로 이동</a>
            <h1>🛡️ AuditAgent 검토 로그</h1>
          </div>
        </header>

        <section class="log-filter-section">
          <div class="filter-group">
            <label for="status-filter">필터:</label>
            <select id="status-filter">
              <option value="ALL">전체 보기</option>
              <option value="PASS">PASS (통과)</option>
              <option value="REJECTED">REJECTED (거부)</option>
            </select>
          </div>
        </section>

        <div class="table-wrapper">
          <table class="data-table" id="logs-table">
            <thead>
              <tr>
                <th>ID</th>
                <th>종목코드</th>
                <th>상태</th>
                <th>생성 일시</th>
                <th>상세 보기</th>
              </tr>
            </thead>
            <tbody>
              ${logRows || '<tr><td colspan="5" class="text-center text-muted">기록된 검토 로그가 없습니다.</td></tr>'}
            </tbody>
          </table>
        </div>

        <div id="prompt-modal" class="modal hidden">
          <div class="modal-content glass-container">
            <div class="modal-header">
              <h3>📄 생성된 프롬프트 상세보기</h3>
              <span class="close-modal-btn" id="close-modal">&times;</span>
            </div>
            <div class="modal-body">
              <textarea id="modal-prompt-text" readonly></textarea>
            </div>
          </div>
        </div>
      </div>
    `;
  },
  init: async () => {
    const filterSelect = document.getElementById('status-filter');
    const table = document.getElementById('logs-table');
    const tbody = table ? table.querySelector('tbody') : null;
    const modal = document.getElementById('prompt-modal');
    const modalText = document.getElementById('modal-prompt-text');
    const modalClose = document.getElementById('close-modal');

    if (filterSelect) {
      filterSelect.addEventListener('change', (e) => {
        const val = e.target.value;
        const rows = tbody.querySelectorAll('tr');
        rows.forEach(row => {
          const badge = row.querySelector('.badge');
          if (!badge) return;
          const status = badge.innerText.trim();
          if (val === 'ALL' || status === val) {
            row.classList.remove('hidden');
          } else {
            row.classList.add('hidden');
          }
        });
      });
    }

    if (tbody) {
      tbody.addEventListener('click', (e) => {
        const btn = e.target.closest('.view-prompt-btn');
        if (btn) {
          const prompt = btn.getAttribute('data-prompt');
          modalText.value = prompt;
          modal.classList.remove('hidden');
        }
      });
    }

    if (modalClose) {
      modalClose.addEventListener('click', () => {
        modal.classList.add('hidden');
      });
    }

    window.addEventListener('click', (e) => {
      if (e.target === modal) {
        modal.classList.add('hidden');
      }
    });
  }
};

export const statusView = {
  path: '/status',
  view: async () => {
    let stats = { total_rows: 0, unique_stocks: 0, total_logs: 0, latest_date: '-' };
    let secReport = { total_vulnerability_events: 0, tier_failures: { WEB: 0, WAS: 0, DB: 0 } };
    try {
      const [sRes, rRes] = await Promise.all([
        fetch(`${API_BASE}/stats`),
        fetch(`${API_BASE}/security/report`)
      ]);
      if (sRes.ok) stats = await sRes.json();
      if (rRes.ok) secReport = await rRes.json();
    } catch (e) {
      console.error('Failed to load status/security data', e);
    }

    return `
      <div class="glass-container main-wrapper">
        <header class="detail-header">
          <div class="header-left">
            <a href="/" data-link class="back-link">◀ 홈으로 이동</a>
            <h1>📡 4-Tier System Health & Security 관제</h1>
          </div>
        </header>

        <section class="detail-grid">
          <div class="detail-card">
            <h3>💾 데이터 저장소 상태 (PostgreSQL DB Tier)</h3>
            <ul class="status-list">
              <li>
                <span class="label">연결 상태:</span>
                <span class="value text-gradient-green">● Online (Retry-Protection Active)</span>
              </li>
              <li>
                <span class="label">적재 테이블:</span>
                <span class="value">public.raw_stock_data, public.prompt_logs, public.market_cap</span>
              </li>
              <li>
                <span class="label">총 레코드 수:</span>
                <span class="value">${formatNumber(stats.total_rows)} 건</span>
              </li>
              <li>
                <span class="label">고유 종목 수:</span>
                <span class="value">${formatNumber(stats.unique_stocks)} 개</span>
              </li>
              <li>
                <span class="label">최근 수집 일자:</span>
                <span class="value">${escapeHtml(stats.latest_date || '-')}</span>
              </li>
            </ul>
          </div>

          <div class="detail-card">
            <h3>🛡️ 4-Tier 통합 보안 및 라이프사이클 관제 (Manager Tier)</h3>
            
            <div class="agent-status-box">
              <div class="agent-info">
                <strong>🌐 WEB Tier AuditAgent</strong>
                <span class="badge-status-sm badge-pass-sm">Active</span>
              </div>
              <p class="text-muted small">오픈 리다이렉트, 원격 스크립트 인젝션 차단 및 정적 자원 검증 (차단 횟수: ${secReport.tier_failures ? secReport.tier_failures.WEB : 0}건)</p>
            </div>

            <div class="agent-status-box">
              <div class="agent-info">
                <strong>⚙️ WAS Tier AuditAgent</strong>
                <span class="badge-status-sm badge-pass-sm">Active</span>
              </div>
              <p class="text-muted small">Path Traversal, XSS 및 REST API 입력 파라미터 유효성 전담 관제 (차단 횟수: ${secReport.tier_failures ? secReport.tier_failures.WAS : 0}건)</p>
            </div>

            <div class="agent-status-box">
              <div class="agent-info">
                <strong>🗄️ DB Tier AuditAgent</strong>
                <span class="badge-status-sm badge-pass-sm">Active</span>
              </div>
              <p class="text-muted small">SQL Injection 패턴 사전 탐구 및 쿼리 파라미터화 검증 (차단 횟수: ${secReport.tier_failures ? secReport.tier_failures.DB : 0}건)</p>
            </div>
          </div>
        </section>
      </div>
    `;
  },
  init: async () => {}
};
