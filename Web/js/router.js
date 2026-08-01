// Client-Side Router for Single Page Application (SPA)
// Uses HTML5 History API (pushState/popstate)

export class Router {
  constructor(routes) {
    this.routes = routes;
    this.rootElem = document.getElementById('app-root');

    document.addEventListener('click', (e) => {
      const link = e.target.closest('[data-link]');
      if (link) {
        e.preventDefault();
        this.navigateTo(link.getAttribute('href'));
      }
    });

    window.addEventListener('popstate', () => this.route());
  }

  navigateTo(url) {
    history.pushState(null, null, url);
    this.route();
  }

  async route() {
    const path = window.location.pathname;
    
    let match = this.routes.find(route => {
      if (typeof route.path === 'string') {
        return route.path === path;
      } else if (route.path instanceof RegExp) {
        return route.path.test(path);
      }
      return false;
    });

    if (!match) {
      match = {
        path: '/404',
        view: () => `
          <div class="glass-container error-panel">
            <h2>⚠️ 404 - 페이지를 찾을 수 없습니다</h2>
            <p>요청하신 경로 <code>${escapeHtml(path)}</code>는 존재하지 않는 페이지입니다.</p>
            <a href="/" data-link class="secondary-btn btn-inline">홈으로 돌아가기</a>
          </div>
        `
      };
    }

    let params = {};
    if (match.path instanceof RegExp) {
      const result = match.path.exec(path);
      if (result && result.length > 1) {
        params.symbol = result[1];
      }
    }

    try {
      this.rootElem.innerHTML = '<div class="loader">로딩 중...</div>';
      const htmlContent = await match.view(params);
      this.rootElem.innerHTML = htmlContent;
      
      if (match.init) {
        await match.init(params);
      }
    } catch (error) {
      console.error('Rendering error:', error);
      this.rootElem.innerHTML = `
        <div class="glass-container error-panel">
          <h2>❌ 렌더링 오류가 발생했습니다</h2>
          <p class="text-muted">${escapeHtml(error.message)}</p>
          <a href="/" data-link class="secondary-btn btn-inline">홈으로 돌아가기</a>
        </div>
      `;
    }
  }
}

export function escapeHtml(unsafe) {
  if (!unsafe) return '';
  return unsafe
    .toString()
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}
