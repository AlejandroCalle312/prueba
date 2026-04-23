(function () {
  'use strict';

  var isLocalHost =
    window.location.hostname === 'localhost' ||
    window.location.hostname === '127.0.0.1';

  if (!isLocalHost) {
    return;
  }

  window.APP_CONFIG = window.APP_CONFIG || {};

  var apiBase = (window.APP_CONFIG.apiBase || '').trim();
  if (!apiBase || /^%%.+%%$/.test(apiBase)) {
    window.APP_CONFIG.apiBase = 'http://localhost:8000';
  }
})();
