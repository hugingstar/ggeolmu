document.addEventListener('DOMContentLoaded', () => {
  const searchBtn = document.getElementById('search-btn');
  const symbolInput = document.getElementById('symbol-input');
  const auditStatus = document.getElementById('audit-status');
  const auditReason = document.getElementById('audit-reason');
  const promptOutput = document.getElementById('prompt-output');
  const copyBtn = document.getElementById('copy-btn');

  const API_BASE = 'http://localhost:8000/api';

  async function fetchPrompt(symbol) {
    // UI 초기화
    searchBtn.disabled = true;
    searchBtn.innerText = '생성 중...';
    auditStatus.className = 'badge-status badge-neutral';
    auditStatus.innerText = '검토 중...';
    auditReason.innerText = '-';
    promptOutput.value = '';
    copyBtn.disabled = true;

    try {
      const res = await fetch(`${API_BASE}/prompt?symbol=${encodeURIComponent(symbol)}`);
      const data = await res.json();

      if (data.is_valid) {
        auditStatus.className = 'badge-status badge-pass';
        auditStatus.innerText = 'PASS (안전함)';
        auditReason.innerText = data.audit_reason;
        promptOutput.value = data.generated_prompt;
        copyBtn.disabled = false;
      } else {
        auditStatus.className = 'badge-status badge-fail';
        auditStatus.innerText = 'REJECTED (차단됨)';
        auditReason.innerText = data.audit_reason;
        promptOutput.value = '에이전트 검토 결과 차단되었습니다. 프롬프트를 생성할 수 없습니다.';
      }
    } catch (err) {
      console.error(err);
      auditStatus.className = 'badge-status badge-fail';
      auditStatus.innerText = 'ERROR';
      auditReason.innerText = 'API 통신 오류가 발생했습니다.';
    } finally {
      searchBtn.disabled = false;
      searchBtn.innerText = '프롬프트 생성';
    }
  }

  searchBtn.addEventListener('click', () => {
    const symbol = symbolInput.value.trim();
    if (!symbol) return;
    fetchPrompt(symbol);
  });

  symbolInput.addEventListener('keypress', (e) => {
    if (e.key === 'Enter') {
      searchBtn.click();
    }
  });

  copyBtn.addEventListener('click', async () => {
    if (promptOutput.value) {
      try {
        await navigator.clipboard.writeText(promptOutput.value);
        const originalText = copyBtn.innerText;
        copyBtn.innerText = '복사 완료!';
        setTimeout(() => copyBtn.innerText = originalText, 2000);
      } catch (err) {
        console.error('클립보드 복사 실패', err);
      }
    }
  });
});
