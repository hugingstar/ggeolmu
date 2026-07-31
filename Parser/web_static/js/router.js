// Client-Side Router for Single Page Application (SPA)
// Uses HTML5 History API (pushState/popstate)

export class Router {
  constructor(routes) {
    this.routes = routes;
    this.rootElem = document.getElementById('app-root');

    // Intercept clicks on links with data-link or local links
    document.addEventListener('click', (e) => {
      const link = e.target.closest('[data-link]');
      if (link) {
        e.preventDefault();
        this.navigateTo(link.getAttribute('href'));
      }
    });

    // Listen for back/forward navigation
    window.addEventListener('popstate', () => this.route());
  }

  // Navigate to a path programmatically
  navigateTo(url) {
    history.pushState(null, null, url);
    this.route();
  }

  // Find matching route and execute its handler
  async route() {
    const path = window.location.pathname;
    
    // Default fallback route (Home)
    let match = this.routes.find(route => {
      if (typeof route.path === 'string') {
        return route.path === path;
      } else if (route.path instanceof RegExp) {
        return route.path.test(path);
      }
      return false;
    });

    if (!match) {
      // 404 fallback
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

    // Extract dynamic params if match path is regex
    let params = {};
    if (match.path instanceof RegExp) {
      const result = match.path.exec(path);
      if (result && result.length > 1) {
        // Assume the first capture group is the primary parameter (e.g. symbol)
        params.symbol = result[1];
      }
    }

    // Render the view
    try {
      this.rootElem.innerHTML = '<div class="loader">로딩 중...</div>';
      const htmlContent = await match.view(params);
      this.rootElem.innerHTML = htmlContent;
      
      // If the view has an initialization function, execute it
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

// Utility function to escape HTML to prevent XSS in client-side routing
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
