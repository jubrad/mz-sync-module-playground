/**
 * mz-sync.js — Frontend sync engine for mz-http.
 *
 * Maintains a local mirror of a Materialize view via SSE or WebSocket.
 * Handles reconnection with changelog-based catchup (falls back to full
 * snapshot if the server has compacted past the client's last progress).
 *
 * Usage:
 *
 *   const sync = new MzSync('http://localhost:8080', 'my_view', {
 *     transport: 'sse',  // or 'ws'
 *   });
 *
 *   sync.on('ready',    ()       => console.log('initial sync complete'));
 *   sync.on('upsert',   (key, value, ts) => { ... });
 *   sync.on('delete',   (key, ts) => { ... });
 *   sync.on('progress', (ts)     => { ... });
 *   sync.on('error',    (err)    => { ... });
 *
 *   sync.connect();
 *
 *   // Read local state (instant, no network).
 *   sync.get('some_key');       // → parsed value or undefined
 *   sync.getAll();              // → Map<string, any>
 *   sync.keys();                // → string[]
 *   sync.size;                  // → number
 *   sync.progress;              // → last progress timestamp
 *
 *   sync.disconnect();
 */

class MzSync {
  /**
   * @param {string} baseUrl — mz-http server origin (e.g. 'http://localhost:8080')
   * @param {string} viewName — view to sync
   * @param {object} opts
   * @param {'sse'|'ws'} [opts.transport='sse']
   * @param {number}     [opts.reconnectMs=2000]
   * @param {boolean}    [opts.autoConnect=false]
   */
  constructor(baseUrl, viewName, opts = {}) {
    this._baseUrl = baseUrl.replace(/\/$/, '');
    this._viewName = viewName;
    this._transport = opts.transport || 'sse';
    this._reconnectMs = opts.reconnectMs ?? 2000;

    /** @type {Map<string, any>} Local mirror of the view state. */
    this.data = new Map();

    /** Last progress timestamp from the server. */
    this.progress = 0;

    /** True after the first catchup_complete / snapshot_complete. */
    this.ready = false;

    this._listeners = {};
    this._conn = null;       // EventSource or WebSocket
    this._reconnectTimer = null;
    this._destroyed = false;

    if (opts.autoConnect) this.connect();
  }

  // ── Public API ──────────────────────────────────────

  connect() {
    this._destroyed = false;
    this._doConnect();
  }

  disconnect() {
    this._destroyed = true;
    clearTimeout(this._reconnectTimer);
    this._closeConn();
  }

  /** Get a single value (parsed JSON). */
  get(key) {
    return this.data.get(key);
  }

  /** Get the full local state as a Map. */
  getAll() {
    return new Map(this.data);
  }

  /** Get all keys. */
  keys() {
    return [...this.data.keys()];
  }

  /** Number of keys. */
  get size() {
    return this.data.size;
  }

  // ── Events ──────────────────────────────────────────

  /**
   * Register an event listener.
   * Events: 'upsert', 'delete', 'progress', 'ready', 'error',
   *         'connected', 'disconnected', 'catchup'
   */
  on(event, fn) {
    (this._listeners[event] ??= []).push(fn);
    return this;
  }

  off(event, fn) {
    const arr = this._listeners[event];
    if (arr) this._listeners[event] = arr.filter(f => f !== fn);
    return this;
  }

  _emit(event, ...args) {
    for (const fn of (this._listeners[event] || [])) {
      try { fn(...args); } catch (e) { console.error('mz-sync listener error:', e); }
    }
  }

  // ── Connection ──────────────────────────────────────

  _doConnect() {
    if (this._destroyed) return;
    this._closeConn();

    // Build URL with catchup params.
    const params = new URLSearchParams();
    if (this.progress > 0) {
      params.set('since', this.progress);
    } else {
      params.set('snapshot', 'true');
    }

    if (this._transport === 'ws') {
      this._connectWS(params);
    } else {
      this._connectSSE(params);
    }
  }

  _connectSSE(params) {
    const url = `${this._baseUrl}/api/sse/${encodeURIComponent(this._viewName)}?${params}`;
    const es = new EventSource(url);
    this._conn = es;

    es.onopen = () => this._emit('connected');
    es.onerror = () => {
      this._emit('error', 'SSE connection error');
      this._scheduleReconnect();
    };

    for (const evType of ['upsert', 'delete', 'progress', 'catchup_complete', 'snapshot_complete']) {
      es.addEventListener(evType, (e) => this._handleMessage(evType, e.data));
    }
  }

  _connectWS(params) {
    const proto = this._baseUrl.startsWith('https') ? 'wss' : 'ws';
    const host = this._baseUrl.replace(/^https?:\/\//, '');
    const url = `${proto}://${host}/api/ws/${encodeURIComponent(this._viewName)}?${params}`;
    const ws = new WebSocket(url);
    this._conn = ws;

    ws.onopen = () => this._emit('connected');
    ws.onclose = () => {
      this._emit('disconnected');
      this._scheduleReconnect();
    };
    ws.onerror = () => this._emit('error', 'WebSocket error');
    ws.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data);
        this._handleMessage(msg.type, e.data);
      } catch (err) {
        this._emit('error', err);
      }
    };
  }

  _closeConn() {
    if (!this._conn) return;
    if (this._conn instanceof EventSource) {
      this._conn.close();
    } else if (this._conn.readyState !== undefined) {
      this._conn.close();
    }
    this._conn = null;
  }

  _scheduleReconnect() {
    if (this._destroyed) return;
    this._closeConn();
    clearTimeout(this._reconnectTimer);
    this._reconnectTimer = setTimeout(() => this._doConnect(), this._reconnectMs);
  }

  // ── Message handling ────────────────────────────────

  _handleMessage(type, rawData) {
    let msg;
    try { msg = typeof rawData === 'string' ? JSON.parse(rawData) : rawData; } catch { return; }

    switch (type || msg.type) {
      case 'upsert': {
        let val;
        try { val = typeof msg.value === 'string' ? JSON.parse(msg.value) : msg.value; }
        catch { val = msg.value; }
        this.data.set(msg.key, val);
        if (msg.timestamp) this.progress = Math.max(this.progress, msg.timestamp);
        this._emit('upsert', msg.key, val, msg.timestamp);
        break;
      }
      case 'delete': {
        this.data.delete(msg.key);
        if (msg.timestamp) this.progress = Math.max(this.progress, msg.timestamp);
        this._emit('delete', msg.key, msg.timestamp);
        break;
      }
      case 'progress': {
        if (msg.timestamp) this.progress = Math.max(this.progress, msg.timestamp);
        this._emit('progress', msg.timestamp);
        break;
      }
      case 'catchup_complete':
      case 'snapshot_complete': {
        if (!this.ready) {
          this.ready = true;
          this._emit('ready');
        }
        this._emit('catchup', msg);
        break;
      }
      case 'lagged': {
        // Server says we missed events. Reconnect with since= to catch up.
        this._emit('error', `lagged: skipped ${msg.skipped} events, reconnecting`);
        this._scheduleReconnect();
        break;
      }
    }
  }
}

// Export for ES modules and plain scripts.
if (typeof module !== 'undefined' && module.exports) {
  module.exports = MzSync;
}
