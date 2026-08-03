// Script for Time Series Clustering Analysis & Cluster Stock Explorer Page

document.addEventListener('DOMContentLoaded', () => {
  const mktBtns = document.querySelectorAll('.mkt-btn');
  const clustersContainer = document.getElementById('clusters-container');
  const topologyBox = document.getElementById('topology-3d-box');
  const elbowChartBox = document.getElementById('elbow-chart-box');
  const summaryText = document.getElementById('cluster-summary-text');

  let currentMarket = 'KOSPI';
  let activeModalCluster = null;
  let currentModalView = 'grid';
  let activeClusterData = null;

  // Render SVG Elbow Curve Chart for optimal k criterion
  function renderElbowChart(elbowData, optimalK) {
    if (!elbowData || elbowData.length === 0) return '';

    const width = 450;
    const height = 160;
    const padding = 25;

    const kVals = elbowData.map(d => d.k);
    const inertias = elbowData.map(d => d.inertia);

    const minK = Math.min(...kVals);
    const maxK = Math.max(...kVals);
    const minIn = Math.min(...inertias) * 0.9;
    const maxIn = Math.max(...inertias) * 1.05;

    const chartW = width - padding * 2;
    const chartH = height - padding * 2;

    const points = elbowData.map(d => {
      const x = padding + ((d.k - minK) / (maxK - minK || 1)) * chartW;
      const y = padding + chartH - ((d.inertia - minIn) / (maxIn - minIn || 1)) * chartH;
      return { k: d.k, inertia: d.inertia, x, y };
    });

    const linePath = points.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x} ${p.y}`).join(' ');

    const nodes = points.map(p => {
      const isOptimal = p.k === optimalK;
      const fillColor = isOptimal ? '#10b981' : '#3b82f6';
      const strokeColor = isOptimal ? '#ffffff' : 'rgba(255,255,255,0.6)';
      const radius = isOptimal ? 7 : 4;
      const label = isOptimal ? ` (최적 k=${p.k})` : '';

      return `
        <circle cx="${p.x}" cy="${p.y}" r="${radius}" fill="${fillColor}" stroke="${strokeColor}" stroke-width="2" />
        <text x="${p.x}" y="${p.y - 10}" fill="${isOptimal ? '#34d399' : 'rgba(255,255,255,0.6)'}" font-size="9" font-weight="${isOptimal ? 'bold' : 'normal'}" text-anchor="middle">k=${p.k}${label}</text>
      `;
    }).join('');

    return `
      <svg viewBox="0 0 ${width} ${height}" class="stock-svg-chart">
        <line x1="${padding}" y1="${height - padding}" x2="${width - padding}" y2="${height - padding}" stroke="rgba(255,255,255,0.1)" />
        <path d="${linePath}" fill="none" stroke="#3b82f6" stroke-width="2.5" stroke-linecap="round" />
        ${nodes}
      </svg>
    `;
  }

  // Render 3D AI Model Topology Cards (Centroid perspective layout)
  function render3DTopology(clusters) {
    if (!clusters || clusters.length === 0) return '';

    return `
      <div class="topology-3d-grid">
        ${clusters.map((c, i) => `
          <div class="topology-node clickable" data-cluster-id="${c.cluster_id}" style="--node-color: ${c.color}; transform: translateZ(${i * 4}px);">
            <div class="node-badge" style="background: ${c.color};">Cluster #${c.cluster_id}</div>
            <div class="node-title">${c.title}</div>
            <div class="node-count">${c.stock_count}개 종목 (${c.ratio_pct || 0}%)</div>
            <span class="click-hint">👆 클릭 시 이동</span>
          </div>
        `).join('')}
      </div>
    `;
  }

  // Modal Render Function
  function renderModalContent(filterQuery = '') {
    const body = document.getElementById('modal-stock-body');
    const countInfo = document.getElementById('modal-stock-count-info');
    if (!body || !activeModalCluster) return;

    const q = filterQuery.toLowerCase().trim();
    const filtered = activeModalCluster.stocks.filter(s => {
      if (!q) return true;
      return (s.name && s.name.toLowerCase().includes(q)) || (s.symbol && s.symbol.toLowerCase().includes(q)) || (s.market && s.market.toLowerCase().includes(q));
    });

    if (countInfo) {
      countInfo.innerText = `총 ${activeModalCluster.stocks.length}개 종목 중 검색결과 ${filtered.length}개 (${activeModalCluster.ratio_pct || 0}% 점유율)`;
    }

    if (filtered.length === 0) {
      body.innerHTML = `<div class="text-muted text-center p-4">"${filterQuery}" 검색 결과가 없습니다.</div>`;
      return;
    }

    if (currentModalView === 'grid') {
      body.innerHTML = `
        <div class="modal-grid-container">
          ${filtered.map(s => `
            <a href="/?symbol=${encodeURIComponent(s.symbol)}" class="modal-stock-chip">
              <span class="msc-name">${s.name}</span>
              <div class="msc-meta">
                <span class="msc-code">${s.symbol}</span>
                <span class="msc-mkt">${s.market || currentMarket}</span>
              </div>
            </a>
          `).join('')}
        </div>
      `;
    } else {
      body.innerHTML = `
        <table class="modal-stock-table">
          <thead>
            <tr>
              <th>종목명</th>
              <th>종목코드</th>
              <th>소속 마켓</th>
              <th>Z-Score 모멘텀</th>
              <th>상세 분석</th>
            </tr>
          </thead>
          <tbody>
            ${filtered.map(s => `
              <tr>
                <td class="st-name"><strong>${s.name}</strong></td>
                <td class="st-code"><code>${s.symbol}</code></td>
                <td><span class="msc-mkt">${s.market || currentMarket}</span></td>
                <td><span style="color: ${activeModalCluster.color}; font-weight: bold;">${activeModalCluster.mean_zscore || '0.0'}</span></td>
                <td><a href="/?symbol=${encodeURIComponent(s.symbol)}" class="table-link-btn">📈 차트 분석</a></td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      `;
    }
  }

  function openStockModal(cluster) {
    activeModalCluster = cluster;
    const modal = document.getElementById('cluster-stock-modal');
    const tag = document.getElementById('modal-cluster-tag');
    const title = document.getElementById('modal-cluster-title');
    const desc = document.getElementById('modal-cluster-desc');
    const searchInput = document.getElementById('modal-stock-search');

    if (!modal) return;

    if (tag) {
      tag.innerText = `Cluster #${cluster.cluster_id}`;
      tag.style.background = `${cluster.color}22`;
      tag.style.color = cluster.color;
    }
    if (title) title.innerText = cluster.title;
    if (desc) desc.innerText = `${cluster.desc} (마켓: ${currentMarket})`;
    if (searchInput) searchInput.value = '';

    renderModalContent('');
    modal.classList.remove('hidden');
    document.body.style.overflow = 'hidden';
  }

  function closeStockModal() {
    const modal = document.getElementById('cluster-stock-modal');
    if (modal) {
      modal.classList.add('hidden');
      document.body.style.overflow = 'auto';
    }
  }

  // Render Cluster Time-Series Trajectory Sparkline Graph
  function renderClusterSparkline(trajectory, color = '#3b82f6') {
    if (!trajectory || trajectory.length === 0) return '';

    const width = 280;
    const height = 65;
    const padding = 10;

    const minVal = Math.min(...trajectory);
    const maxVal = Math.max(...trajectory);
    const range = maxVal - minVal || 1;

    const chartW = width - padding * 2;
    const chartH = height - padding * 2;

    const points = trajectory.map((val, i) => {
      const x = padding + (i / Math.max(1, trajectory.length - 1)) * chartW;
      const y = padding + chartH - ((val - minVal) / range) * chartH;
      return { x, y, val };
    });

    const linePath = points.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x.toFixed(1)} ${p.y.toFixed(1)}`).join(' ');
    const areaPath = `${linePath} L ${points[points.length - 1].x.toFixed(1)} ${height - padding} L ${points[0].x.toFixed(1)} ${height - padding} Z`;
    const endPoint = points[points.length - 1];
    const gradId = `grad-${color.replace('#', '')}`;

    return `
      <div class="cluster-sparkline-box">
        <div class="sparkline-header">
          <span class="sparkline-title">📈 시계열 파형 (Centroid Trajectory)</span>
          <span class="sparkline-val" style="color: ${color};">Z: ${endPoint.val >= 0 ? '+' : ''}${endPoint.val.toFixed(2)}</span>
        </div>
        <svg viewBox="0 0 ${width} ${height}" class="sparkline-svg">
          <defs>
            <linearGradient id="${gradId}" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stop-color="${color}" stop-opacity="0.4"/>
              <stop offset="100%" stop-color="${color}" stop-opacity="0.0"/>
            </linearGradient>
          </defs>
          <path d="${areaPath}" fill="url(#${gradId})" />
          <path d="${linePath}" fill="none" stroke="${color}" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" />
          <circle cx="${endPoint.x}" cy="${endPoint.y}" r="3.5" fill="${color}" stroke="#ffffff" stroke-width="1.5" />
        </svg>
      </div>
    `;
  }

  // Load and Render Clustering Data from WAS API
  async function loadClusteringData(market = 'ALL') {
    clustersContainer.innerHTML = '<div class="loader">PostgreSQL DB 시계열 클러스터링 소속 종목 조회 중...</div>';
    currentMarket = market;

    const elbowSubtext = document.getElementById('elbow-subtext');
    const elbowReasonNote = document.getElementById('elbow-reason-note');

    try {
      const res = await fetch(`/api/clustering?market=${encodeURIComponent(market)}`);
      if (!res.ok) throw new Error('API response failed');

      const data = await res.json();
      activeClusterData = data;

      if (summaryText) {
        summaryText.innerText = `총 ${data.total_clusters}개 시계열 군집 분류 완료 (${data.market} 시장, 총 ${data.total_market_stocks}개 종목 분석)`;
      }

      if (elbowSubtext) {
        elbowSubtext.innerText = `${data.market} 마켓 DB 데이터 기반 왜곡(Inertia) 감소율 곡선 연산 결과 최적 k=${data.optimal_k} 개수가 도출되었습니다.`;
      }

      if (elbowReasonNote) {
        elbowReasonNote.innerHTML = `🎯 <strong>선택 기준:</strong> ${data.optimal_k_reason}`;
      }

      if (elbowChartBox) {
        elbowChartBox.innerHTML = renderElbowChart(data.elbow_data, data.optimal_k);
      }

      if (topologyBox) {
        topologyBox.innerHTML = render3DTopology(data.clusters);
      }

      if (!data.clusters || data.clusters.length === 0) {
        clustersContainer.innerHTML = '<div class="text-muted text-center p-4">선택한 시장의 클러스터링 데이터가 없습니다.</div>';
        return;
      }

      clustersContainer.innerHTML = data.clusters.map(cluster => {
        const initialStocks = cluster.stocks.slice(0, 10); // 5행(약 10개) 표출 제한

        const stockBadges = initialStocks.map(s => `
          <a href="/?symbol=${encodeURIComponent(s.symbol)}" class="stock-badge">
            <span class="sb-name">${s.name}</span>
            <div class="sb-meta">
              <span class="sb-code">${s.symbol}</span>
              <span class="sb-mkt">${s.market || currentMarket}</span>
            </div>
          </a>
        `).join('');

        const sparklineSvg = renderClusterSparkline(cluster.trajectory, cluster.color);
        const ratioPct = cluster.ratio_pct !== undefined ? cluster.ratio_pct : 0;

        return `
          <div class="cluster-card" id="cluster-card-${cluster.cluster_id}" style="border-left: 4px solid ${cluster.color};">
            <div>
              <div class="cluster-card-header">
                <div>
                  <span class="cluster-id-tag" style="background: ${cluster.color}22; color: ${cluster.color};">
                    Cluster #${cluster.cluster_id}
                  </span>
                  <h3 class="cluster-title">${cluster.title}</h3>
                  <p class="cluster-desc">${cluster.desc}</p>
                </div>
                <div class="cluster-metrics-badge">
                  <span class="stock-count-text">${cluster.stock_count}개 종목</span>
                  <span class="stock-ratio-text">(${ratioPct}%)</span>
                </div>
              </div>

              <!-- Cluster Stock Ratio Progress Bar -->
              <div class="cluster-ratio-box">
                <div class="ratio-label-row">
                  <span>마켓 내 점유 비율</span>
                  <span style="color: ${cluster.color}; font-weight: bold;">${ratioPct}%</span>
                </div>
                <div class="cluster-ratio-bar-bg">
                  <div class="cluster-ratio-bar-fill" style="width: ${Math.min(100, Math.max(2, ratioPct))}%; background: ${cluster.color};"></div>
                </div>
              </div>

              <!-- Feature Tags -->
              <div class="feature-chip-group">
                <span class="feature-chip">⚡ 평균 Z-Score: <strong>${cluster.mean_zscore || '0.0'}</strong></span>
                <span class="feature-chip">📊 변동성: <strong>${cluster.volatility || '0.0%'}</strong></span>
              </div>

              ${sparklineSvg}
            </div>

            <div class="cluster-stocks-wrapper" style="margin-top: 1rem;">
              <div class="stocks-label-row">
                <span class="stocks-label">📌 포함된 소속 종목 (${cluster.stock_count}개 / ${ratioPct}%):</span>
                <button class="open-modal-btn" data-cluster-id="${cluster.cluster_id}">🔍 전체 소속 종목 팝업 관제</button>
              </div>
              <div class="stocks-badge-grid" id="stocks-grid-${cluster.cluster_id}">
                ${stockBadges}
              </div>
            </div>
          </div>
        `;
      }).join('');

    } catch (e) {
      console.error('Failed to load clustering data', e);
      clustersContainer.innerHTML = `<div class="error-panel">클러스터링 데이터 조회 중 오류가 발생했습니다: ${e.message}</div>`;
    }
  }

  // 3D Topology Click Handler (Smooth Scroll to Cluster Card)
  if (topologyBox) {
    topologyBox.addEventListener('click', (e) => {
      const node = e.target.closest('.topology-node');
      if (node) {
        const cid = node.getAttribute('data-cluster-id');
        const targetCard = document.getElementById(`cluster-card-${cid}`);
        if (targetCard) {
          targetCard.scrollIntoView({ behavior: 'smooth', block: 'center' });
          targetCard.classList.add('pulse-highlight');
          setTimeout(() => targetCard.classList.remove('pulse-highlight'), 2000);
        }
      }
    });
  }

  // Cluster Card Open Modal Handler
  if (clustersContainer) {
    clustersContainer.addEventListener('click', (e) => {
      const openBtn = e.target.closest('.open-modal-btn');
      if (openBtn && activeClusterData) {
        const cid = parseInt(openBtn.getAttribute('data-cluster-id'), 10);
        const cluster = activeClusterData.clusters.find(c => c.cluster_id === cid);
        if (cluster) openStockModal(cluster);
      }

      // 더보기 버튼 클릭 이벤트 리스너(제거됨)
    });
  }

  // Modal Controls Event Listeners
  const closeBtn = document.getElementById('modal-close-btn');
  if (closeBtn) closeBtn.addEventListener('click', closeStockModal);

  const modalBackdrop = document.getElementById('cluster-stock-modal');
  if (modalBackdrop) {
    modalBackdrop.addEventListener('click', (e) => {
      if (e.target === modalBackdrop) closeStockModal();
    });
  }

  const modalSearch = document.getElementById('modal-stock-search');
  if (modalSearch) {
    modalSearch.addEventListener('input', (e) => {
      renderModalContent(e.target.value);
    });
  }

  const viewGridBtn = document.getElementById('view-mode-grid');
  const viewTableBtn = document.getElementById('view-mode-table');
  if (viewGridBtn && viewTableBtn) {
    viewGridBtn.addEventListener('click', () => {
      currentModalView = 'grid';
      viewGridBtn.classList.add('active');
      viewTableBtn.classList.remove('active');
      const q = modalSearch ? modalSearch.value : '';
      renderModalContent(q);
    });

    viewTableBtn.addEventListener('click', () => {
      currentModalView = 'table';
      viewTableBtn.classList.add('active');
      viewGridBtn.classList.remove('active');
      const q = modalSearch ? modalSearch.value : '';
      renderModalContent(q);
    });
  }

  // Market Filter Event Handlers
  mktBtns.forEach(btn => {
    btn.addEventListener('click', () => {
      mktBtns.forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      const mkt = btn.getAttribute('data-mkt');
      loadClusteringData(mkt);
    });
  });

  // Initial Load
  loadClusteringData('ALL');
});
