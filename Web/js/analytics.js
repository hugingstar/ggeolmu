// Analytics Page Script for Data Query Monitoring

document.addEventListener('DOMContentLoaded', async () => {
  const statTotal = document.getElementById('stat-total-queries');
  const statUnique = document.getElementById('stat-unique-queried');
  const statSqlPct = document.getElementById('stat-sql-pct');
  const statLatency = document.getElementById('stat-latency');
  const tbody = document.getElementById('analytics-tbody');

  try {
    const res = await fetch('/api/analytics');
    if (!res.ok) return;

    const data = await res.json();

    if (statTotal) statTotal.innerText = Number(data.total_queries || 0).toLocaleString() + ' 건';
    if (statUnique) statUnique.innerText = Number(data.unique_stocks_queried || 0).toLocaleString() + ' 개';
    if (statSqlPct && data.query_balance) statSqlPct.innerText = `${data.query_balance.sql_queries_pct}%`;
    if (statLatency && data.query_balance) statLatency.innerText = `${data.query_balance.latency_avg_ms} ms`;

    if (tbody && data.top_queried_stocks) {
      if (data.top_queried_stocks.length === 0) {
        tbody.innerHTML = '<tr><td colspan="4" class="text-center text-muted">기록된 조회 데이터가 없습니다.</td></tr>';
        return;
      }

      tbody.innerHTML = data.top_queried_stocks.map((item, index) => `
        <tr>
          <td><strong>#${index + 1}</strong></td>
          <td><a href="/?symbol=${item.symbol}" class="log-symbol-link">${(item.display_name || item.symbol).split(' (')[0]}</a></td>
          <td><span class="badge badge-purple">${item.count} 회</span></td>
          <td>${item.last_queried ? item.last_queried.substring(0, 19).replace('T', ' ') : '-'}</td>
        </tr>
      `).join('');
    }
  } catch (e) {
    console.error('Failed to load analytics', e);
  }
});
