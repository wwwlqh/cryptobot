// Shared UTC timestamp normalization for the plain bridge and the Babel
// modules. SQLite timestamps are UTC-naive strings; ISO strings with an
// explicit offset keep their own offset.
(function () {
  function parseUtcTimestamp(value) {
    if (value == null) return null;
    if (typeof value === 'number') {
      var numeric = new Date(value);
      return Number.isNaN(numeric.getTime()) ? null : numeric;
    }
    var text = String(value).trim();
    if (!text) return null;
    if (/^\d{4}-\d{2}-\d{2} /.test(text)) text = text.replace(' ', 'T');
    if (/^\d{4}-\d{2}-\d{2}T/.test(text)
        && !/(?:Z|[+-]\d{2}:?\d{2})$/i.test(text)) text += 'Z';
    var parsed = new Date(text);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  function settlementTimestamp(row) {
    if (!row) return null;
    if (row.exit_kind === 'stop_loss' && row.exit_ts) return row.exit_ts;
    return row.resolution_ts || row.bet_ts;
  }

  function settlementMs(row) {
    var parsed = parseUtcTimestamp(settlementTimestamp(row));
    return parsed ? parsed.getTime() : null;
  }

  window.CB_TIME = {
    parseUtcTimestamp: parseUtcTimestamp,
    settlementTimestamp: settlementTimestamp,
    settlementMs: settlementMs,
  };
})();
