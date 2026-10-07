// Authenticated dashboard mutation helpers. Keep this plain JavaScript so the
// operator client can test and use the exact request contract before Babel
// compiles the JSX pages.
(function () {
  function cookieValue(name) {
    var prefix = name + '=';
    var parts = (document.cookie || '').split(';');
    for (var i = 0; i < parts.length; i += 1) {
      var item = parts[i].trim();
      if (!item.startsWith(prefix)) continue;
      try {
        return decodeURIComponent(item.slice(prefix.length));
      } catch (e) {
        return item.slice(prefix.length);
      }
    }
    return null;
  }

  function post(path, body) {
    var csrf = cookieValue('csrf_token');
    return fetch(path, {
      method: 'POST',
      credentials: 'same-origin',
      headers: Object.assign(
        { 'Content-Type': 'application/json' },
        csrf ? { 'X-CSRF-Token': csrf } : {}
      ),
      body: JSON.stringify(body || {}),
    }).then(function (resp) {
      return resp.json().catch(function () { return {}; }).then(function (payload) {
        if (!resp.ok) {
          var err = new Error(payload.detail || payload.reason || ('request failed ' + resp.status));
          err.payload = payload;
          throw err;
        }
        return payload;
      });
    });
  }

  window.CB_API = { post: post };
})();
