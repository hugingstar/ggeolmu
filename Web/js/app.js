// Main Dashboard Script (Stock Chart with Dynamic Time Period & Buy/Sell Signals & Live News Panel)

document.addEventListener('DOMContentLoaded', () => {
  const symbolInput = document.getElementById('symbol-input');
  const searchBtn = document.getElementById('search-btn');
  const autocompleteBox = document.getElementById('autocomplete-box');
  const stockTitle = document.getElementById('stock-title');
  const stockBadges = document.getElementById('stock-badges');
  const chartArea = document.getElementById('chart-area');
  const stockTbody = document.getElementById('stock-tbody');
  const promptOutput = document.getElementById('prompt-output');
  const copyPromptBtn = document.getElementById('copy-prompt-btn');
  const newsListBox = document.getElementById('news-list-box');
  const quickTags = document.querySelectorAll('.tag-btn');
  const periodBtns = document.querySelectorAll('.period-btn');

  let autocompleteTimeout = null;
  let currentSymbol = '005930';
  let currentDays = 30;

  function formatNumber(num) {
    if (num === null || num === undefined) return '-';
    return Number(num).toLocaleString();
  }

  function formatChange(val) {
    if (val === null || val === undefined) return '-';
    const pct = (val * 100).toFixed(2);
    return pct > 0 ? `+${pct}%` : `${pct}%`;
  }

  // Draw Dynamic SVG Line Chart with BUY (🟢) & SELL (🔴) Signal Markers (Supports 5D, 1M, 3M, 6M, 1Y, ALL)
  function renderSvgChartWithSignals(history) {
    if (!history || history.length === 0) {
      return `<div class="no-chart-data">가격 차트 데이터를 불러올 수 없습니다.</div>`;
    }

    const closes = history.map(h => h.close).filter(c => c !== null);
    if (closes.length === 0) {
      return `<div class="no-chart-data">가격 데이터가 부족합니다.</div>`;
    }

    const minVal = Math.min(...closes) * 0.99;
    const maxVal = Math.max(...closes) * 1.01;
    const range = maxVal - minVal || 1;

    const width = 650;
    const height = 240;
    const padding = 35;

    const chartWidth = width - padding * 2;
    const chartHeight = height - padding * 2;

    const points = history.map((h, i) => {
      if (h.close === null) return null;
      const x = padding + (i / Math.max(1, history.length - 1)) * chartWidth;
      const y = padding + chartHeight - ((h.close - minVal) / range) * chartHeight;
      return { index: i, x, y, val: h.close, date: h.date, signal: h.signal, vol: h.volume };
    }).filter(p => p !== null);

    const linePath = points.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x} ${p.y}`).join(' ');
    const areaPath = `${linePath} L ${points[points.length - 1].x} ${height - padding} L ${points[0].x} ${height - padding} Z`;

    const gridLines = [];
    for (let i = 0; i <= 3; i++) {
      const y = padding + (i / 3) * chartHeight;
      const price = maxVal - (i / 3) * range;
      gridLines.push(`
        <line x1="${padding}" y1="${y}" x2="${width - padding}" y2="${y}" stroke="rgba(255,255,255,0.06)" stroke-dasharray="3 3" />
        <text x="${width - padding + 5}" y="${y + 4}" fill="rgba(255,255,255,0.4)" font-size="9" font-family="monospace">${formatNumber(Math.round(price))}</text>
      `);
    }

    // X-axis Sampling for date labels to avoid label overlap on long series (e.g. 1Y, ALL)
    const step = Math.max(1, Math.floor(points.length / 6));

    const markers = points.map((p) => {
      let markerEl = '';
      if (p.signal === 'BUY') {
        markerEl = `
          <g transform="translate(${p.x}, ${p.y - 12})">
            <circle cx="0" cy="0" r="8" fill="#10b981" />
            <text x="0" y="3" fill="#ffffff" font-size="7" font-weight="bold" text-anchor="middle">매수</text>
          </g>
        `;
      } else if (p.signal === 'SELL') {
        markerEl = `
          <g transform="translate(${p.x}, ${p.y + 14})">
            <circle cx="0" cy="0" r="8" fill="#f43f5e" />
            <text x="0" y="3" fill="#ffffff" font-size="7" font-weight="bold" text-anchor="middle">매도</text>
          </g>
        `;
      }

      const showDateLabel = (p.index % step === 0) || (p.index === points.length - 1);
      const dateText = showDateLabel ? `<text x="${p.x}" y="${height - 8}" fill="rgba(255,255,255,0.5)" font-size="9" text-anchor="middle">${p.date.substring(5)}</text>` : '';
      const dotCircle = (points.length <= 60 || p.signal !== 'NEUTRAL') ? `<circle cx="${p.x}" cy="${p.y}" r="3.5" fill="#3b82f6" stroke="#ffffff" stroke-width="1" />` : '';

      return `
        ${dateText}
        ${dotCircle}
        ${markerEl}
      `;
    }).join('');

    return `
      <svg viewBox="0 0 ${width} ${height}" class="stock-svg-chart">
        <defs>
          <linearGradient id="chartGrad" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stop-color="#3b82f6" stop-opacity="0.35"/>
            <stop offset="100%" stop-color="#3b82f6" stop-opacity="0.0"/>
          </linearGradient>
        </defs>
        ${gridLines.join('')}
        <path d="${areaPath}" fill="url(#chartGrad)" />
        <path d="${linePath}" fill="none" stroke="#3b82f6" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" />
        ${markers}
      </svg>
    `;
  }

  // Fetch News Feed
  async function loadNewsFeed(queryStr) {
    newsListBox.innerHTML = '<div class="loader">최신 관련 뉴스 수집 중...</div>';
    try {
      const res = await fetch(`/api/news?query=${encodeURIComponent(queryStr)}`);
      if (res.ok) {
        const news = await res.json();
        if (news.length === 0) {
          newsListBox.innerHTML = '<div class="text-muted text-center p-3">관련 뉴스가 없습니다.</div>';
          return;
        }

        newsListBox.innerHTML = news.map(item => `
          <a href="${item.link}" target="_blank" rel="noopener noreferrer" class="news-card">
            <div class="news-title">${item.title}</div>
            <div class="news-meta">
              <span class="news-source">📰 ${item.source}</span>
              <span class="news-date">${item.pubDate}</span>
            </div>
          </a>
        `).join('');
      } else {
        newsListBox.innerHTML = '<div class="text-muted text-center p-3">뉴스를 가져올 수 없습니다.</div>';
      }
    } catch (e) {
      console.error('Failed to load news', e);
      newsListBox.innerHTML = '<div class="text-muted text-center p-3">뉴스 수집 중 오류 발생</div>';
    }
  }

  // Execute Stock Search & Render with Dynamic Time Period (days)
  async function executeSearch(symbolOrName, days = currentDays) {
    if (!symbolOrName) return;

    currentDays = days;
    chartArea.innerHTML = '<div class="loader">PostgreSQL DB 주가 이력 및 매수/매도 시그널 연산 중...</div>';
    stockTbody.innerHTML = '<tr><td colspan="5" class="text-center text-muted">데이터베이스 조회 중...</td></tr>';
    promptOutput.value = '4-Tier Multi-Agent 프롬프트 생성 중...';

    let resolvedSymbol = symbolOrName.trim().toUpperCase();

    try {
      const [detailRes, promptRes] = await Promise.all([
        fetch(`/api/stock/${encodeURIComponent(resolvedSymbol)}?days=${days}`),
        fetch(`/api/prompt?symbol=${encodeURIComponent(resolvedSymbol)}`)
      ]);

      let detailData = null;
      let promptData = null;

      if (detailRes.ok) detailData = await detailRes.json();
      if (promptRes.ok) promptData = await promptRes.json();

      if (detailData) {
        currentSymbol = detailData.symbol;
        stockTitle.innerText = `${detailData.name} (${detailData.symbol})`;
        stockBadges.innerHTML = `
          <span class="badge badge-success">● PostgreSQL DB ${detailData.history.length}일치 이력 연동 완료</span>
        `;

        chartArea.innerHTML = renderSvgChartWithSignals(detailData.history);

        stockTbody.innerHTML = detailData.history.slice(-30).map(h => {
          let sigBadge = '<span class="sig-tag sig-neutral">보유</span>';
          if (h.signal === 'BUY') sigBadge = '<span class="sig-tag sig-buy">🟢 매수</span>';
          if (h.signal === 'SELL') sigBadge = '<span class="sig-tag sig-sell">🔴 매도</span>';

          return `
            <tr>
              <td>${h.date}</td>
              <td>${formatNumber(h.close)}</td>
              <td class="${h.change > 0 ? 'text-green' : h.change < 0 ? 'text-red' : ''}">${formatChange(h.change)}</td>
              <td>${formatNumber(h.volume)}</td>
              <td>${sigBadge}</td>
            </tr>
          `;
        }).join('');

        loadNewsFeed(detailData.name || detailData.symbol);

      } else {
        stockTitle.innerText = `검색 결과: ${resolvedSymbol}`;
        chartArea.innerHTML = `<div class="error-panel">종목 정보를 찾을 수 없습니다. (종목 코드/이름을 확인하세요)</div>`;
        stockTbody.innerHTML = `<tr><td colspan="5" class="text-center text-muted">데이터가 없습니다.</td></tr>`;
        loadNewsFeed(resolvedSymbol);
      }

      if (promptData && promptData.is_valid) {
        promptOutput.value = promptData.generated_prompt;
      } else if (promptData) {
        promptOutput.value = `[검토 거부] ${promptData.audit_reason}`;
      }

    } catch (e) {
      console.error('Search error', e);
      chartArea.innerHTML = `<div class="error-panel">시스템 오류가 발생했습니다: ${e.message}</div>`;
    }
  }

  // Time Period Selector Buttons Event Handlers
  periodBtns.forEach(btn => {
    btn.addEventListener('click', () => {
      periodBtns.forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      const days = parseInt(btn.getAttribute('data-days'), 10) || 30;
      executeSearch(currentSymbol, days);
    });
  });

  // Event Listeners
  if (searchBtn) {
    searchBtn.addEventListener('click', () => {
      executeSearch(symbolInput.value, currentDays);
    });
  }

  if (symbolInput) {
    symbolInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') executeSearch(symbolInput.value, currentDays);
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
          const res = await fetch(`/api/search?q=${encodeURIComponent(val)}`);
          if (res.ok) {
            const list = await res.json();
            if (list.length === 0) {
              autocompleteBox.classList.add('hidden');
              return;
            }
            autocompleteBox.innerHTML = list.map(item => `
              <div class="autocomplete-item" data-symbol="${item.symbol}">
                <span class="ac-name">${item.name}</span>
                <span class="ac-symbol">${item.symbol}</span>
              </div>
            `).join('');
            autocompleteBox.classList.remove('hidden');
          }
        } catch (err) {
          console.error(err);
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
        executeSearch(sym, currentDays);
      }
    });
  }

  quickTags.forEach(btn => {
    btn.addEventListener('click', () => {
      const sym = btn.getAttribute('data-symbol');
      symbolInput.value = sym;
      executeSearch(sym, currentDays);
    });
  });

  if (copyPromptBtn && promptOutput) {
    copyPromptBtn.addEventListener('click', () => {
      if (!promptOutput.value) return;
      promptOutput.select();
      navigator.clipboard.writeText(promptOutput.value);
      copyPromptBtn.innerText = '복사 완료! ✅';
      setTimeout(() => { copyPromptBtn.innerText = '복사하기'; }, 2000);
    });
  }

  const urlParams = new URLSearchParams(window.location.search);
  const initialSym = urlParams.get('symbol');
  if (initialSym) {
    symbolInput.value = initialSym;
    executeSearch(initialSym, currentDays);
  } else {
    executeSearch('005930', currentDays);
  }
});
