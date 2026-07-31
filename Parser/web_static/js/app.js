import { Router } from './router.js';
import { dashboardView, stockDetailView, logsView, statusView } from './views.js';

document.addEventListener('DOMContentLoaded', () => {
  // Define routes matching our planning document
  const routes = [
    dashboardView,
    stockDetailView,
    logsView,
    statusView
  ];

  // Instantiate and bind the SPA Router to the window scope
  // so views can trigger programmatic transitions
  window.routerInstance = new Router(routes);
  
  // Trigger the initial routing path based on current window.location
  window.routerInstance.route();
});
