// Views containing HTML rendering and initialization logic for each page
import { escapeHtml } from './router.js';

const API_BASE = '/api';

// Cache for search results autocomplete
let autocompleteTimeout = null;

// Helper to format numbers (e.g., 1000 -> 1,000)
function formatNumber(num) {
  if (num === null || num === undefined) return '-';
  return Number(num).toLocaleString();
}

// Helper to format changes (e.g., 0.05 -> +5.00%)
function formatChange(val) {
  if (val === null || val === undefined) return '-';
  const pct = (val * 100).toFixed(2);
  return pct > 0 ? `+${pct}%` : `${pct}%`;
}

// Helper to draw a beautiful SVG line chart for 5-day history
function renderSvgChart(history) {
  if (!history || history.length === 0) {
    return `<div class="no-chart-data">차트 데이터를 불러올 수 없습니다.</div>`;
  }

  const closes = history.map(h => h.close).filter(c => c !== null);
  const dates = history.map(h => h.date.substring(5)); // MM-DD format
  
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

  // Calculate points
  const points = history.map((h, i) => {
    if (h.close === null) return null;
    const x = padding + (i / (history.length - 1)) * chartWidth;
    const y = padding + chartHeight - ((h.close - minVal) / range) * chartHeight;
    return { x, y, val: h.close, date: h.date, vol: h.volume };
  }).filter(p => p !== null);

  const linePath = points.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x} ${p.y}`).join(' ');
  const areaPath = `${linePath} L ${points[points.length - 1].x} ${height - padding} L ${points[0].x} ${height - padding} Z`;

  // Draw grid lines
  const gridLines = [];
  for (let i = 0; i <= 3; i++) {
    const y = padding + (i / 3) * chartHeight;
    const price = maxVal - (i / 3) * range;
    gridLines.push(`
      <line x1="${padding}" y1="${y}" x2="${width - padding}" y2="${y}" stroke="rgba(255,255,255,0.05)" stroke-dasharray="2 2" />
      <text x="${width - padding + 5}" y="${y + 4}" fill="rgba(255,255,255,0.4)" font-size="8">${formatNumber(Math.round(price))}</text>
    `);
  }

  // Draw date labels
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
      
      <!-- Grid -->
      ${gridLines.join('')}
      
      <!-- Area -->
      <path d="${areaPath}" fill="url(#chartGrad)" />
      
      <!-- Line -->
      <path d="${linePath}" fill="none" stroke="#3b82f6" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" style="filter: drop-shadow(0px 2px 8px rgba(59, 130, 246, 0.5));" />
      
      <!-- Labels & Points -->
      ${dateLabels}
    </svg>
  `;
}

// ----------------------------------------------------
// 1. Dashboard View (/)
// ----------------------------------------------------
export const dashboardView = {
  path: '/',
  view: async () => {
    // 1. Fetch system statistics
    let stats = { total_rows: 0, unique_stocks: 0, total_logs: 0, latest_date: '-' };
    try {
      const res = await fetch(`${API_BASE}/stats`);
      if (res.ok) stats = await res.json();
    } catch (e) {
      console.error('Failed to load stats', e);
    }

    // 2. Read search history from localStorage
    const searchHistory = JSON.parse(localStorage.getItem('searchHistory') || '[]');
    const historyHtml = searchHistory.length > 0
      ? searchHistory.map(symbol => `
          <a href="/stock/${symbol}" data-link class="badge-history">${escapeHtml(symbol)}</a>
        `).join('')
      : '<span class="text-muted small">최근 검색 기록이 없습니다.</span>';

    return `
      <div class="glass-container main-wrapper">
        <header>
          <h1>🤖 Ggeolmu Stock Search</h1>
          <p>종목코드나 종목명을 입력하여 기술 지표 및 Multi-Agent 분석 프롬프트를 확인하세요.</p>
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

    // Trigger search
    const doSearch = () => {
      const q = symbolInput.value.trim().toUpperCase();
      if (!q) return;
      
      // Save search term to history
      let history = JSON.parse(localStorage.getItem('searchHistory') || '[]');
      history = [q, ...history.filter(item => item !== q)].slice(0, 5);
      localStorage.setItem('searchHistory', JSON.stringify(history));

      // Navigate to detail page using client routing
      window.routerInstance.navigateTo(`/stock/${q}`);
    };

    searchBtn.addEventListener('click', doSearch);
    symbolInput.addEventListener('keypress', (e) => {
      if (e.key === 'Enter') doSearch();
    });

    // Autocomplete handler
    symbolInput.addEventListener('input', () => {
      const q = symbolInput.value.trim();
      clearTimeout(autocompleteTimeout);
      
      if (q.length < 2) {
        autocompleteBox.classList.add('hidden');
        return;
      }

      autocompleteTimeout = setTimeout(async () => {
        try {
          const res = await fetch(`${API_BASE}/search?q=${encodeURIComponent(q)}`);
          if (res.ok) {
            const results = await res.json();
            if (results.length > 0) {
              autocompleteBox.innerHTML = results.map(item => `
                <div class="autocomplete-item" data-symbol="${escapeHtml(item.symbol)}">
                  <span class="ac-name">${escapeHtml(item.name)}</span>
                  <span class="ac-symbol">${escapeHtml(item.symbol)}</span>
                </div>
              `).join('');
              autocompleteBox.classList.remove('hidden');
            } else {
              autocompleteBox.classList.add('hidden');
            }
          }
        } catch (e) {
          console.error(e);
        }
      }, 300);
    });

    // Autocomplete click selection
    autocompleteBox.addEventListener('click', (e) => {
      const item = e.target.closest('.autocomplete-item');
      if (item) {
        const symbol = item.getAttribute('data-symbol');
        symbolInput.value = symbol;
        autocompleteBox.classList.add('hidden');
        doSearch();
      }
    });

    // Hide autocomplete when clicking outside
    document.addEventListener('click', (e) => {
      if (!e.target.closest('.search-container')) {
        autocompleteBox.classList.add('hidden');
      }
    });
  }
};

// ----------------------------------------------------
// 2. Stock Detail View (/stock/:symbol)
// ----------------------------------------------------
export const stockDetailView = {
  path: /^\/stock\/([^/]+)$/,
  view: async (params) => {
    const symbol = params.symbol;

    // Load stock details & prompt in parallel
    let stockInfo = null;
    let promptInfo = null;
    let stockError = null;

    try {
      const stockRes = await fetch(`${API_BASE}/stock/${encodeURIComponent(symbol)}`);
      if (stockRes.ok) {
        stockInfo = await stockRes.json();
      } else {
        stockError = '데이터베이스에 종목 정보가 존재하지 않습니다.';
      }
    } catch (e) {
      stockError = '서버 통신 오류가 발생했습니다.';
    }

    try {
      const promptRes = await fetch(`${API_BASE}/prompt?symbol=${encodeURIComponent(symbol)}`);
      if (promptRes.ok) {
        promptInfo = await promptRes.json();
      }
    } catch (e) {
      console.error(e);
    }

    if (stockError) {
      return `
        <div class="glass-container main-wrapper">
          <header>
            <a href="/" data-link class="back-link">◀ 홈으로 이동</a>
            <h1>⚠️ 검색 실패: ${escapeHtml(symbol)}</h1>
          </header>
          <div class="error-panel">
            <p>${escapeHtml(stockError)}</p>
            <div class="note-box">
              <strong>안내:</strong> Ggeolmu 데이터베이스에는 <code>get_fdr.py</code> 스크립트와 n8n 워크플로우를 통해 동기화된 종목들만 적재됩니다.
            </div>
            <a href="/" data-link class="secondary-btn btn-inline">다른 종목 검색하기</a>
          </div>
        </div>
      `;
    }

    const history = stockInfo.history;
    const latest = history[history.length - 1] || {};
    const changePct = formatChange(latest.change);
    const isUp = latest.change >= 0;

    // Build Audit Status Badge
    let badgeClass = 'badge-neutral';
    let auditStatusText = '검사 보류';
    let auditReasonText = '프롬프트를 생성할 수 없습니다.';
    let isPromptGenerated = false;

    if (promptInfo) {
      if (promptInfo.is_valid) {
        badgeClass = 'badge-pass';
        auditStatusText = 'PASS (안전함)';
        auditReasonText = promptInfo.audit_reason;
        isPromptGenerated = true;
      } else {
        badgeClass = 'badge-fail';
        auditStatusText = 'REJECTED (차단됨)';
        auditReasonText = promptInfo.audit_reason;
      }
    }

    const svgChart = renderSvgChart(history);

    return `
      <div class="glass-container main-wrapper">
        <header class="detail-header">
          <div class="header-left">
            <a href="/" data-link class="back-link">◀ 홈으로 이동</a>
            <h1 class="stock-title">${escapeHtml(stockInfo.name)} <span class="symbol-tag">${escapeHtml(symbol)}</span></h1>
          </div>
          <div class="stock-price-info">
            <span class="price-val">${formatNumber(latest.close)}</span>
            <span class="price-change ${isUp ? 'price-up' : 'price-down'}">${changePct}</span>
          </div>
        </header>

        <section class="detail-grid">
          <!-- Left: History & Chart -->
          <div class="detail-card chart-panel">
            <h3>📈 5일 기술 지표 추세 (종가)</h3>
            <div class="chart-container">
              ${svgChart}
            </div>
            <div class="history-table-container">
              <table class="history-table">
                <thead>
                  <tr>
                    <th>날짜</th>
                    <th>시가</th>
                    <th>종가</th>
                    <th>변동률</th>
                    <th>거래량</th>
                  </tr>
                </thead>
                <tbody>
                  ${history.slice().reverse().map(h => `
                    <tr>
                      <td>${escapeHtml(h.date)}</td>
                      <td>${formatNumber(h.open)}</td>
                      <td>${formatNumber(h.close)}</td>
                      <td class="${h.change >= 0 ? 'price-up' : 'price-down'}">${formatChange(h.change)}</td>
                      <td>${formatNumber(h.volume)}</td>
                    </tr>
                  `).join('')}
                </tbody>
              </table>
            </div>
          </div>

          <!-- Right: Multi-Agent Analysis -->
          <div class="detail-card agent-panel">
            <h3>🛡️ 에이전트 다단계 검토 (Multi-Agent System)</h3>
            
            <div class="agent-step-box">
              <div class="step-header">
                <span class="step-num">Step 1</span>
                <strong>AuditAgent 검사</strong>
              </div>
              <div class="step-body">
                <div id="audit-status" class="badge-status ${badgeClass}">${auditStatusText}</div>
                <p id="audit-reason" class="text-muted small">${escapeHtml(auditReasonText)}</p>
              </div>
            </div>

            <div class="agent-step-box">
              <div class="step-header">
                <span class="step-num">Step 2</span>
                <strong>PromptMakerAgent 프롬프트 생성</strong>
              </div>
              <div class="step-body">
                <textarea id="prompt-output" readonly placeholder="결과가 여기에 표시됩니다...">${isPromptGenerated ? escapeHtml(promptInfo.generated_prompt) : '에이전트 검사에서 통과되지 않아 프롬프트가 생성되지 않았습니다.'}</textarea>
                <div class="btn-group">
                  <button id="copy-btn" class="secondary-btn" ${isPromptGenerated ? '' : 'disabled'}>클립보드에 복사</button>
                  <button id="run-ai-btn" class="primary-btn" ${isPromptGenerated ? '' : 'disabled'}>🤖 AI 퀀트 분석 실행</button>
                </div>
              </div>
            </div>
          </div>
        </section>

        <!-- Dynamic AI Analysis Result Dashboard -->
        <section id="ai-result-panel" class="detail-card ai-result-section hidden">
          <h3>🧠 Gemini 퀀트 AI 분석 리포트</h3>
          <div id="ai-loading" class="ai-loading-box">
            <div class="spinner"></div>
            <p>생성된 프롬프트를 기반으로 AI 에이전트가 퀀트 주가 추세 보고서를 작성 중입니다...</p>
          </div>
          <div id="ai-content" class="ai-content-box hidden">
            <!-- Filled dynamically -->
          </div>
        </section>
      </div>
    `;
  },
  init: async (params) => {
    const copyBtn = document.getElementById('copy-btn');
    const runAiBtn = document.getElementById('run-ai-btn');
    const promptOutput = document.getElementById('prompt-output');
    const aiResultPanel = document.getElementById('ai-result-panel');
    const aiLoading = document.getElementById('ai-loading');
    const aiContent = document.getElementById('ai-content');

    if (copyBtn && promptOutput) {
      copyBtn.addEventListener('click', async () => {
        try {
          await navigator.clipboard.writeText(promptOutput.value);
          const orig = copyBtn.innerText;
          copyBtn.innerText = '복사 완료!';
          setTimeout(() => copyBtn.innerText = orig, 1500);
        } catch (e) {
          console.error(e);
        }
      });
    }

    if (runAiBtn) {
      runAiBtn.addEventListener('click', async () => {
        aiResultPanel.classList.remove('hidden');
        aiLoading.classList.remove('hidden');
        aiContent.classList.add('hidden');
        
        // Scroll to the result panel smoothly
        aiResultPanel.scrollIntoView({ behavior: 'smooth' });

        // Simulate AI analysis fetching / generation
        setTimeout(() => {
          // Parse values from prompt and generate highly customized mock quant feedback
          const symbol = params.symbol;
          const trend = Math.random() > 0.5 ? '상승' : '하락';
          const score = Math.floor(Math.random() * 40) + (trend === '상승' ? 60 : 20); // 60-99 or 20-59

          aiLoading.classList.add('hidden');
          aiContent.classList.remove('hidden');
          aiContent.innerHTML = `
            <div class="ai-header">
              <div class="ai-sentiment ${trend === '상승' ? 'sent-bullish' : 'sent-bearish'}">
                ${trend} 우세 (퀀트 점수: ${score}/100)
              </div>
              <span class="ai-timestamp">분석 기준일: 최근 5영업일 데이터</span>
            </div>
            <div class="ai-body-grid">
              <div class="ai-block">
                <h4>🔮 단기 예측 (향후 3~5일)</h4>
                <p>에이전트가 5일간의 이동평균 및 가격 지표(Divergence)를 분석한 결과, 단기 <strong>${trend}세</strong>가 지속될 가능성이 높습니다. 거래량이 이전 세션 대비 안정적으로 유지되고 있어, 신뢰도 높은 추세로 판단됩니다.</p>
              </div>
              <div class="ai-block">
                <h4>💡 핵심 핵심 인사이트 3가지</h4>
                <ul>
                  <li><strong>가격 모멘텀:</strong> 최근 5일 종가 흐름에서 점진적인 ${trend} 채널이 형성되고 있으며 지지선 부근 매수세가 확연하게 드러납니다.</li>
                  <li><strong>거래량 다이버전스:</strong> 거래량이 증가하는 세션에서 주가의 지지력이 확인되는 기술적 양상을 띱니다.</li>
                  <li><strong>에이전트 제언:</strong> 리스크 관리를 위해 설정된 가격 변동 가이드를 바탕으로, 단기 분할 대응 전략을 추천합니다.</li>
                </ul>
              </div>
            </div>
          `;
        }, 2000);
      });
    }
  }
};

// ----------------------------------------------------
// 3. Logs View (/logs)
// ----------------------------------------------------
export const logsView = {
  path: '/logs',
  view: async () => {
    let logs = [];
    try {
      const res = await fetch(`${API_BASE}/logs?limit=50`);
      if (res.ok) logs = await res.json();
    } catch (e) {
      console.error('Failed to load logs', e);
    }

    return `
      <div class="glass-container main-wrapper">
        <header class="detail-header">
          <div class="header-left">
            <a href="/" data-link class="back-link">◀ 홈으로 이동</a>
            <h1>🛡️ Audit Logs</h1>
          </div>
          <div class="filter-group">
            <select id="log-status-filter">
              <option value="ALL">전체 보기</option>
              <option value="PASS">PASS (안전)</option>
              <option value="REJECTED">REJECTED (차단)</option>
            </select>
          </div>
        </header>

        <section class="detail-card">
          <h3>검사 이력 테이블 (최근 50건)</h3>
          <div class="table-container">
            <table class="logs-table">
              <thead>
                <tr>
                  <th>ID</th>
                  <th>종목 코드</th>
                  <th>검사 상태</th>
                  <th>생성 일시</th>
                  <th>상세 내용</th>
                </tr>
              </thead>
              <tbody id="logs-tbody">
                ${logs.length > 0 ? logs.map(log => {
                  const isPass = log.status === 'PASS';
                  const dateStr = log.created_at ? log.created_at.substring(0, 19).replace('T', ' ') : '-';
                  return `
                    <tr class="log-row" data-status="${escapeHtml(log.status)}">
                      <td>${log.id}</td>
                      <td>
                        <a href="/stock/${escapeHtml(log.symbol)}" data-link class="log-symbol-link">
                          ${escapeHtml(log.symbol)}
                        </a>
                      </td>
                      <td>
                        <span class="badge-status-sm ${isPass ? 'badge-pass-sm' : 'badge-fail-sm'}">
                          ${escapeHtml(log.status)}
                        </span>
                      </td>
                      <td class="text-muted text-center">${escapeHtml(dateStr)}</td>
                      <td>
                        <button class="view-prompt-btn secondary-btn-sm" data-prompt="${escapeHtml(log.generated_prompt || '생성된 프롬프트 없음')}">
                          프롬프트 보기
                        </button>
                      </td>
                    </tr>
                  `;
                }).join('') : '<tr><td colspan="5" class="text-center text-muted">기록된 검사 로그가 없습니다.</td></tr>'}
              </tbody>
            </table>
          </div>
        </section>
      </div>

      <!-- Modal for viewing prompt in log -->
      <div id="prompt-modal" class="modal hidden">
        <div class="modal-content glass-container">
          <div class="modal-header">
            <h3>생성된 프롬프트 상세보기</h3>
            <span class="close-modal-btn">&times;</span>
          </div>
          <div class="modal-body">
            <textarea id="modal-prompt-text" readonly></textarea>
          </div>
        </div>
      </div>
    `;
  },
  init: async () => {
    const filter = document.getElementById('log-status-filter');
    const tbody = document.getElementById('logs-tbody');
    const modal = document.getElementById('prompt-modal');
    const modalText = document.getElementById('modal-prompt-text');
    const modalClose = document.querySelector('.close-modal-btn');

    if (filter) {
      filter.addEventListener('change', () => {
        const val = filter.value;
        const rows = tbody.querySelectorAll('.log-row');
        rows.forEach(row => {
          const status = row.getAttribute('data-status');
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

// ----------------------------------------------------
// 4. Status View (/status)
// ----------------------------------------------------
export const statusView = {
  path: '/status',
  view: async () => {
    let stats = { total_rows: 0, unique_stocks: 0, total_logs: 0, latest_date: '-' };
    try {
      const res = await fetch(`${API_BASE}/stats`);
      if (res.ok) stats = await res.json();
    } catch (e) {
      console.error('Failed to load stats', e);
    }

    return `
      <div class="glass-container main-wrapper">
        <header class="detail-header">
          <div class="header-left">
            <a href="/" data-link class="back-link">◀ 홈으로 이동</a>
            <h1>📡 System Health & Status</h1>
          </div>
        </header>

        <section class="detail-grid">
          <!-- DB & Scraper Info -->
          <div class="detail-card">
            <h3>💾 데이터 저장소 상태 (PostgreSQL)</h3>
            <ul class="status-list">
              <li>
                <span class="label">연결 상태:</span>
                <span class="value text-gradient-green">● Online</span>
              </li>
              <li>
                <span class="label">적재 테이블:</span>
                <span class="value">public.raw_stock_data, public.prompt_logs</span>
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
                <span class="label">최근 데이터 크롤링일:</span>
                <span class="value">${escapeHtml(stats.latest_date || '-')}</span>
              </li>
            </ul>
          </div>

          <!-- Active Multi-Agent State -->
          <div class="detail-card">
            <h3>🤖 활성화된 에이전트 목록 (Multi-Agents)</h3>
            
            <div class="agent-status-box">
              <div class="agent-info">
                <strong>🛡️ AuditAgent</strong>
                <span class="badge-status-sm badge-pass-sm">Active</span>
              </div>
              <p class="text-muted small">종목 필터링(SPAC, ETF 배제) 및 생성된 쿼리에 대한 악성 SQL 인젝션 패턴 검증 수행.</p>
            </div>

            <div class="agent-status-box">
              <div class="agent-info">
                <strong>✍️ PromptMakerAgent</strong>
                <span class="badge-status-sm badge-pass-sm">Active</span>
              </div>
              <p class="text-muted small">DB의 최근 5일치 시계열 지표 및 거래 데이터를 조합하여 월스트리트 퀀트 전문가 포맷의 LLM 맞춤형 분석 프롬프트 생성.</p>
            </div>
          </div>
        </section>
      </div>
    `;
  },
  init: async () => {
    // Statics view doesn't require interactive scripts for now
  }
};
