(function () {
  'use strict';

  if (document.getElementById('global-nav')) {
    return;
  }

  var host = window.location.hostname || 'localhost';
  var isLocal = host === 'localhost' || host === '127.0.0.1';
  var base3000 = isLocal ? 'http://' + host + ':' + window.location.port : window.location.origin;
  var path = window.location.pathname;

  var links = [
    { href: base3000 + '/home/index.html', label: 'Menu', match: function () { return path === '/' || path.indexOf('/home/') >= 0; } },
    { href: base3000 + '/tickets-by-hour/tickets-by-hour.html?v=2', label: 'Service Demand Patterns', match: function () { return path.indexOf('tickets-by-hour.html') >= 0; } },
    { href: base3000 + '/tickets-per-agent/index.html?v=2', label: 'Agent Workload & SLA', match: function () { return path.indexOf('/tickets-per-agent/') >= 0; } },
    { href: base3000 + '/ticket-lifecycle/index.html', label: 'Resolution Timeline', match: function () { return path.indexOf('/ticket-lifecycle/') >= 0; } },
    { href: base3000 + '/score-engine/index.html', label: 'Team Performance Forecast', match: function () { return path.indexOf('/score-engine/') >= 0; } },
    { href: base3000 + '/ticket-routing/index.html', label: 'Escalation Intelligence', match: function () { return path.indexOf('/ticket-routing/') >= 0; } },
    { href: base3000 + '/priority-audit/index.html', label: 'Priority Audit', match: function () { return path.indexOf('/priority-audit/') >= 0; } },
    { href: base3000 + '/reassignment-ratio/index.html', label: 'Reassignment Ratio', match: function () { return path.indexOf('/reassignment-ratio/') >= 0; } },
    { href: base3000 + '/tickets-per-language/index.html', label: 'Tickets per Language', match: function () { return path.indexOf('/tickets-per-language/') >= 0; } },
  ];

  var nav = document.createElement('nav');
  nav.id = 'global-nav';
  nav.className = 'global-nav';
  nav.setAttribute('aria-label', 'Global navigation');

  var inner = document.createElement('div');
  inner.className = 'global-nav__inner';

  var brand = document.createElement('div');
  brand.className = 'global-nav__brand';
  brand.textContent = 'SRF-AXSA';
  inner.appendChild(brand);

  var linkWrap = document.createElement('div');
  linkWrap.className = 'global-nav__links';

  links.forEach(function (item) {
    var a = document.createElement('a');
    a.href = item.href;
    a.textContent = item.label;
    if (typeof item.match === 'function' && item.match()) {
      a.classList.add('is-active');
      a.setAttribute('aria-current', 'page');
    }
    linkWrap.appendChild(a);
  });

  inner.appendChild(linkWrap);
  nav.appendChild(inner);
  document.body.insertBefore(nav, document.body.firstChild);
})();




