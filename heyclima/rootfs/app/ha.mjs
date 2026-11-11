// Relay zur Home-Assistant-Schnittstelle (Stufe 2; reines Node, keine Abhängigkeiten). Standard: nur Anzeige. Mit der Option control
// (P3b) zusätzlich ein eng begrenzter Schreibweg (siehe WRITE_SERVICES und checkWrite).
//
// Der Browser spricht über /ha/ws (WebSocket unter dem Ingress-Pfad) ein auf das Lesen verengtes Stück des
// Home-Assistant-Protokolls. Der Proxy meldet sich selbst bei Home Assistant an (Supervisor-Token, ws://supervisor/core/websocket);
// der Token verlässt den Proxy nie. Durch kommt nur eine Allowlist:
//   Browser -> HA:  auth (nur lokal, wird nicht weitergegeben), ping, get_states, subscribe_events (nur state_changed)
//   HA -> Browser:  neu aufgebaut aus Zustand und Ereignis der Domäne climate, nur bekannte Attribute, ohne context (user_id)
//   Nur mit control: call_service, ausschließlich Domäne climate und vier Dienste für eine einzelne, dem Browser gemeldete Entität
//   (checkWrite); der Proxy baut die Anfrage neu auf, begrenzt die Rate und meldet nur ok oder einen festen Fehlercode zurück.
// Alles andere (render_template, config/..., andere Ereignisarten, andere Dienste) wird abgewiesen.
import crypto from "node:crypto";

export const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/** Domänen, die der Browser sieht. sensor.* ist für Stufe 2 (Lesen) nicht nötig: current_temperature steht am climate-Zustand. */
export const DOMAINS = ["climate"];
/** Attribute eines climate-Zustands, die durchgehen (alles andere fällt weg) */
export const ATTRIBUTES = [
  "friendly_name", "hvac_modes", "hvac_mode", "hvac_action", "temperature", "current_temperature", "target_temp_high", "target_temp_low",
  "min_temp", "max_temp", "target_temp_step", "fan_mode", "fan_modes", "swing_mode", "swing_modes", "preset_mode", "preset_modes",
  "current_humidity", "humidity", "supported_features", "temperature_unit",
];
/** Größe einer Nachricht vom Browser (die erlaubten sind winzig) */
const MAX_BROWSER_MESSAGE = 4096;
/** unbeantwortete Anfragen je Verbindung (schützt Home Assistant vor einer Flut von get_states) */
const MAX_PENDING = 8;
/** Ungesendete Bytes je Browser-Verbindung, ab denen sie geschlossen wird (ein Browser, der nicht liest) */
const MAX_UNSENT = 1 << 20;
/** Schreibweg (nur mit control): die einzigen Dienste und je Dienst das eine erlaubte Feld in service_data. Domäne ist immer climate. */
export const WRITE_SERVICES = Object.freeze({
  set_hvac_mode: Object.freeze(["hvac_mode"]),
  set_temperature: Object.freeze(["temperature"]),
  set_fan_mode: Object.freeze(["fan_mode"]),
  set_swing_mode: Object.freeze(["swing_mode"]),
});
/** Betriebsarten, die die App kennt (haAdapter.ts); erlaubt ist davon nur, was die Entität selbst in hvac_modes meldet */
export const HVAC_MODES = Object.freeze(["off", "auto", "cool", "heat", "dry", "fan_only", "heat_cool"]);
/** Felder einer Schreibanfrage des Browsers; jedes andere führt zur Abweisung */
const WRITE_FIELDS = ["id", "type", "domain", "service", "service_data", "target"];
/** Grenzen des Schreibwegs: je Verbindung und Sekunde, insgesamt je Minute, Wartezeit auf Home Assistant */
const DEFAULT_RATE = { perSecond: 5, perMinute: 60 };
const WRITE_TIMEOUT_MS = 10_000;
/** Betriebsart einer Entität höchstens alle 3 s umschalten (Schutz des Verdichters; gilt für alle Verbindungen zusammen) */
const MODE_GAP_MS = 3000;
/** Entitäten, die sich der Relay je Verbindung merkt (ein Haus hat nicht mehr; schützt nur den Speicher) */
const MAX_ENTITIES = 1000;
/** Hier darf der Supervisor-Token hin: der Supervisor-Proxy, nie ein anderer Host. Loopback nur in Tests (HA_ALLOW_LOOPBACK=1). */
const LOOPBACK = ["127.0.0.1", "localhost", "[::1]"];

/** Prüft die Adresse der Home-Assistant-Schnittstelle, bevor der Token je dorthin geht */
export function checkUpstreamUrl(url, env = process.env) {
  let u;
  try { u = new URL(url); } catch { throw new Error("HA_WS_URL ist keine Adresse"); }
  const ok = u.hostname === "supervisor" || (env.HA_ALLOW_LOOPBACK === "1" && LOOPBACK.includes(u.hostname));
  if (u.protocol !== "ws:" || u.username || u.password || !ok) throw new Error("HA_WS_URL muss ws://supervisor/… sein (der Token geht nur dorthin)");
  return url;
}

// ---------------------------------------------------------------------------------------------------------------------
// Filter (rein, ohne Netz)

export function isAllowedEntity(id) {
  return typeof id === "string" && /^[a-z0-9_]+\.[a-z0-9_]+$/.test(id) && DOMAINS.includes(id.split(".")[0]);
}

const isObject = (v) => typeof v === "object" && v !== null && !Array.isArray(v);

/** Zustand einer erlaubten Entität ohne context und mit bekannten Attributen; sonst null */
export function cleanState(s) {
  if (!isObject(s) || !isAllowedEntity(s.entity_id)) return null;
  const attributes = {};
  if (isObject(s.attributes)) for (const key of ATTRIBUTES) if (key in s.attributes) attributes[key] = s.attributes[key];
  const out = { entity_id: s.entity_id, state: typeof s.state === "string" ? s.state : "unknown", attributes };
  if (typeof s.last_updated === "string") out.last_updated = s.last_updated;
  return out;
}

export function cleanStates(list) {
  return Array.isArray(list) ? list.map(cleanState).filter(Boolean) : [];
}

/** state_changed einer erlaubten Entität (new_state null = Entität entfernt); sonst null */
export function cleanEvent(event) {
  if (!isObject(event) || event.event_type !== "state_changed" || !isObject(event.data)) return null;
  const id = event.data.entity_id;
  if (!isAllowedEntity(id)) return null;
  const data = { entity_id: id, new_state: event.data.new_state == null ? null : cleanState(event.data.new_state) };
  if (data.new_state === null && event.data.new_state != null) return null; // unlesbarer Zustand
  const out = { event_type: "state_changed", data };
  return out;
}

const refuse = (id, code, message) => ({ id, type: "result", success: false, error: { code, message } });

/** Zähler für „höchstens max in spanMs“ (has fragt, add zählt; Abgewiesenes zählt nicht). now: monotone Zeit in ms. */
export function createWindow(max, spanMs) {
  const times = [];
  const prune = (now) => { while (times.length && now - times[0] >= spanMs) times.shift(); };
  return {
    has(now) { prune(now); return times.length < max; },
    add(now) { times.push(now); },
  };
}

const finite = (v) => typeof v === "number" && Number.isFinite(v);
const list = (v) => (Array.isArray(v) ? v : []);

/**
 * Schreibanfrage (call_service) des Browsers. Erlaubt ist nur: control ein; domain climate; ein Dienst aus WRITE_SERVICES; genau
 * die Felder id, type, domain, service, service_data, target; target nur { entity_id } mit EINER Kennung, die dem Browser in dieser
 * Verbindung als climate-Entität gemeldet wurde (session.entities); service_data nur das eine Feld des Dienstes, mit einem Wert,
 * den die Entität selbst anbietet (Betriebsart, Lüfter, Schwenken aus ihren Listen) oder der im Bereich min_temp..max_temp der
 * Entität liegt (endliche Zahl). Ergebnis: { forward } (neu aufgebaut, nur mit diesen Feldern) oder { reply } (Fehlercode, nichts
 * geht weiter). Zuletzt die Ratenbegrenzung; nur was sonst durchginge, verbraucht Kontingent.
 * target_temp_high/low bietet der Relay bewusst nicht an: die App kennt nur einen Sollwert (haAdapter.ts), und ein Bereich
 * (heat_cool) braucht eine eigene Oberfläche.
 */
function checkWrite(msg, session, now) {
  const id = msg.id;
  if (session.control !== true) return { reply: refuse(id, "not_allowed", "Home Assistant ist hier nur lesbar.") };
  if (msg.domain !== "climate" || typeof msg.service !== "string" || !Object.hasOwn(WRITE_SERVICES, msg.service)) return { reply: refuse(id, "not_allowed", "Dieser Dienst ist nicht erlaubt.") };
  const invalid = () => ({ reply: refuse(id, "invalid_request", "Ungültige Anfrage.") });
  if (Object.keys(msg).some((k) => !WRITE_FIELDS.includes(k))) return invalid();
  const target = msg.target;
  if (!isObject(target) || Object.keys(target).length !== 1 || typeof target.entity_id !== "string") return invalid();
  const attrs = session.entities?.get(target.entity_id);
  if (!attrs) return { reply: refuse(id, "unknown_entity", "Unbekanntes Klimagerät.") };
  const field = WRITE_SERVICES[msg.service][0];
  const data = msg.service_data;
  if (!isObject(data) || Object.keys(data).length !== 1 || !Object.hasOwn(data, field)) return invalid();
  const value = data[field];
  let ok;
  if (field === "hvac_mode") ok = typeof value === "string" && HVAC_MODES.includes(value) && list(attrs.hvac_modes).includes(value);
  else if (field === "temperature") ok = finite(value) && finite(attrs.min_temp) && finite(attrs.max_temp) && value >= attrs.min_temp && value <= attrs.max_temp;
  else if (field === "fan_mode") ok = typeof value === "string" && list(attrs.fan_modes).includes(value);
  else ok = typeof value === "string" && list(attrs.swing_modes).includes(value);
  if (!ok) return invalid();
  // zuerst „zu viele offene Anfragen“: eine Anfrage, die ohnehin nicht weitergeht, verbraucht kein Kontingent
  if (session.busy?.()) return { reply: refuse(id, "busy", "Zu viele offene Anfragen.") };
  const slow = () => ({ reply: refuse(id, "rate_limited", "Zu viele Befehle, bitte kurz warten.") });
  const gap = session.modeGapMs ?? 0;
  const gated = msg.service === "set_hvac_mode" && gap > 0 && session.modeAt;
  if (gated) { const last = session.modeAt.get(target.entity_id); if (last !== undefined && now - last < gap) return slow(); }
  const windows = [session.writes, session.total].filter(Boolean);
  if (!windows.every((w) => w.has(now))) return slow();
  for (const w of windows) w.add(now);
  if (gated) {
    session.modeAt.set(target.entity_id, now);
    if (session.modeAt.size > MAX_ENTITIES) for (const [k, t] of session.modeAt) if (now - t >= gap) session.modeAt.delete(k);
  }
  return { forward: { id, type: "call_service", domain: "climate", service: msg.service, service_data: { [field]: value }, target: { entity_id: target.entity_id } } };
}

/**
 * Prüft eine Nachricht des Browsers nach der Anmeldung. Ergebnis: { forward } (Anfrage an Home Assistant, neu aufgebaut, nur
 * mit den erlaubten Feldern) oder { reply } (Antwort an den Browser, nichts geht weiter). session: { lastId, subscribed, control,
 * entities, writes, total, busy, modeGapMs, modeAt }; ohne control (Vorgabe) wird nie geschrieben.
 */
export function checkRequest(msg, session, now = performance.now()) {
  if (!isObject(msg) || !Number.isSafeInteger(msg.id) || msg.id <= session.lastId) return { close: "ungültige Nachricht" };
  session.lastId = msg.id;
  const id = msg.id;
  if (msg.type === "ping") return { forward: { id, type: "ping" } };
  if (msg.type === "get_states") return { forward: { id, type: "get_states" } };
  if (msg.type === "subscribe_events") {
    if (msg.event_type !== "state_changed") return { reply: refuse(id, "not_allowed", "Nur state_changed ist erlaubt.") };
    if (session.subscribed) return { reply: refuse(id, "not_allowed", "Schon abonniert.") };
    session.subscribed = true;
    return { forward: { id, type: "subscribe_events", event_type: "state_changed" } };
  }
  if (msg.type === "call_service") return checkWrite(msg, session, now);
  return { reply: refuse(id, "not_allowed", "Home Assistant ist hier nur lesbar.") };
}

// ---------------------------------------------------------------------------------------------------------------------
// WebSocket-Rahmen (RFC 6455), nur das, was der Relay braucht

/** Rahmen zum Senden; mask: Zufallsmaske (Client), sonst unmaskiert (Server) */
export function encodeFrame(opcode, payload = Buffer.alloc(0), mask = false) {
  const len = payload.length;
  const head = len < 126 ? Buffer.from([0x80 | opcode, len]) : len < 65536 ? Buffer.from([0x80 | opcode, 126, len >> 8, len & 255]) : (() => {
    const b = Buffer.alloc(10); b[0] = 0x80 | opcode; b[1] = 127; b.writeBigUInt64BE(BigInt(len), 2); return b;
  })();
  if (!mask) return Buffer.concat([head, payload]);
  const key = crypto.randomBytes(4);
  head[1] |= 0x80;
  const masked = Buffer.from(payload);
  for (let i = 0; i < masked.length; i++) masked[i] ^= key[i & 3];
  return Buffer.concat([head, key, masked]);
}

/**
 * Setzt eingehende Bytes zu Nachrichten zusammen (Fragmente, Steuerrahmen). expectMask: ein Server verlangt maskierte
 * Rahmen. onMessage({ opcode, payload }); bei einem Fehler wirft push() einen Error mit .code (Close-Code).
 */
export class FrameParser {
  constructor({ maxPayload = 65536, expectMask = true, onMessage }) {
    Object.assign(this, { maxPayload, expectMask, onMessage });
    this.buf = Buffer.alloc(0);
    this.fragments = [];
    this.fragmentOpcode = 0;
    this.fragmentSize = 0;
  }
  fail(code, text) { const e = new Error(text); e.code = code; throw e; }
  push(chunk) {
    this.buf = this.buf.length ? Buffer.concat([this.buf, chunk]) : chunk;
    for (;;) {
      const b = this.buf;
      if (b.length < 2) return;
      const fin = (b[0] & 0x80) !== 0, opcode = b[0] & 0x0f, masked = (b[1] & 0x80) !== 0;
      if (b[0] & 0x70) this.fail(1002, "RSV-Bits");
      if (masked !== this.expectMask) this.fail(1002, "Maske");
      let len = b[1] & 0x7f, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) {
        if (b.length < 10) return;
        const big = b.readBigUInt64BE(2);
        if (big > BigInt(this.maxPayload)) this.fail(1009, "zu groß");
        len = Number(big); off = 10;
      }
      const control = opcode >= 0x8;
      if (control && (!fin || len > 125)) this.fail(1002, "Steuerrahmen");
      if (len > this.maxPayload) this.fail(1009, "zu groß");
      if (b.length < off + (masked ? 4 : 0) + len) return;
      let payload;
      if (masked) {
        const key = b.subarray(off, off + 4);
        payload = Buffer.from(b.subarray(off + 4, off + 4 + len));
        for (let i = 0; i < payload.length; i++) payload[i] ^= key[i & 3];
        off += 4;
      } else payload = Buffer.from(b.subarray(off, off + len));
      this.buf = b.subarray(off + len);
      if (control) { this.onMessage({ opcode, payload }); continue; }
      if (opcode === 0) { // Fortsetzung
        if (!this.fragmentOpcode) this.fail(1002, "Fortsetzung ohne Anfang");
        this.fragmentSize += payload.length;
        if (this.fragmentSize > this.maxPayload) this.fail(1009, "zu groß");
        this.fragments.push(payload);
        if (fin) { const full = Buffer.concat(this.fragments); const op = this.fragmentOpcode; this.fragments = []; this.fragmentOpcode = 0; this.fragmentSize = 0; this.onMessage({ opcode: op, payload: full }); }
        continue;
      }
      if (opcode !== 1 && opcode !== 2) this.fail(1002, "Opcode");
      if (this.fragmentOpcode) this.fail(1002, "Anfang in Fragmenten");
      if (fin) this.onMessage({ opcode, payload });
      else { this.fragmentOpcode = opcode; this.fragments = [payload]; this.fragmentSize = payload.length; }
    }
  }
}

/** Antwort auf den Upgrade-Aufruf des Browsers; false (und 400) bei einem ungültigen Aufruf */
export function acceptHandshake(req, socket) {
  const key = req.headers["sec-websocket-key"];
  if (req.headers["sec-websocket-version"] !== "13" || typeof key !== "string" || !/^[A-Za-z0-9+/]{22}==$/.test(key)) {
    socket.end("HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n");
    return false;
  }
  const accept = crypto.createHash("sha1").update(key + WS_GUID).digest("base64");
  socket.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\n\r\n`);
  return true;
}

// ---------------------------------------------------------------------------------------------------------------------
// Relay: ein Browser-Socket, eine eigene Verbindung zu Home Assistant

const text = (obj) => encodeFrame(0x1, Buffer.from(JSON.stringify(obj)));

/**
 * Nimmt den Browser-Socket nach dem Handshake. options: token (Supervisor-Token), upstreamUrl, WebSocketImpl (Vorgabe: das
 * globale WebSocket von Node), authTimeoutMs (so lange wartet der Relay auf auth vom Browser), connectTimeoutMs (Anmeldung bei Home
 * Assistant), pingMs (Herzschlag; ohne Pong auf zwei Herzschläge wird geschlossen), control (Schreibweg frei, Vorgabe aus),
 * rateLimits ({ perSecond } je Verbindung), totalWindow (gemeinsamer Zähler aller Verbindungen, createWindow), writeTimeoutMs (so lange
 * wartet ein Schreibbefehl auf Home Assistant), modeGapMs und modeAt (Mindestabstand je Entität beim Umschalten der Betriebsart und die gemeinsame
 * Liste der letzten Umschaltungen aller Verbindungen; 0: kein Abstand). Gibt eine Funktion zum Schließen zurück.
 */
export function relay(socket, head, { token, upstreamUrl, WebSocketImpl = globalThis.WebSocket, authTimeoutMs = 10_000, connectTimeoutMs = 10_000, pingMs = 25_000, control = false, rateLimits = DEFAULT_RATE, totalWindow = null, writeTimeoutMs = WRITE_TIMEOUT_MS, modeGapMs = MODE_GAP_MS, modeAt = new Map() }) {
  let phase = "auth"; // auth (wartet auf auth vom Browser), connecting (Home Assistant meldet an), ready
  let closed = false;
  let upstream = null;
  let timer = null;
  const pending = new Map(); // id -> type der offenen Anfragen
  let subscription = null;
  const entities = new Map(); // climate-Entitäten, die dem Browser gemeldet wurden: Kennung -> Attribute (nur mit control)
  const writeTimers = new Map(); // id -> Zeitgeber der offenen Schreibbefehle
  const writable = control === true;
  const session = { lastId: 0, subscribed: false, control: writable, entities, writes: createWindow(rateLimits.perSecond, 1000), total: totalWindow, busy: () => pending.size >= MAX_PENDING, modeGapMs, modeAt };
  const remember = (st) => { if (entities.has(st.entity_id) || entities.size < MAX_ENTITIES) entities.set(st.entity_id, st.attributes); };
  const stopTimers = () => { for (const t of writeTimers.values()) clearTimeout(t); writeTimers.clear(); };

  let missedPongs = 0;

  const send = (obj) => {
    if (closed || socket.destroyed) return;
    socket.write(text(obj));
    if (socket.writableLength > MAX_UNSENT) close(1013); // der Browser liest nicht: nicht weiter im Speicher sammeln
  };
  const close = (code = 1000) => {
    if (closed) return;
    closed = true;
    clearTimeout(timer);
    clearInterval(heartbeat);
    stopTimers();
    try { upstream?.close(); } catch { /* schon zu */ }
    if (!socket.destroyed) { const p = Buffer.alloc(2); p.writeUInt16BE(code); socket.end(encodeFrame(0x8, p)); }
    setTimeout(() => socket.destroy(), 1000).unref();
  };
  const reject = (message) => { send({ type: "auth_invalid", message }); close(1008); };

  const heartbeat = setInterval(() => {
    if (closed || socket.destroyed) return;
    if (missedPongs >= 2) return close(1001); // zwei Herzschläge ohne Pong: tot
    missedPongs++;
    socket.write(encodeFrame(0x9));
  }, pingMs);
  heartbeat.unref();
  // wer sich nicht meldet, belegt keinen Platz: ohne auth nach authTimeoutMs zu
  timer = setTimeout(() => { if (phase === "auth") reject("Anmeldung fehlt."); }, authTimeoutMs);

  function connectUpstream() {
    phase = "connecting";
    if (!token) return reject("Kein Zugang zu Home Assistant (homeassistant_api fehlt in der App-Konfiguration).");
    try { upstream = new WebSocketImpl(upstreamUrl); } catch { return reject("Home Assistant nicht erreichbar."); }
    timer = setTimeout(() => reject("Home Assistant antwortet nicht."), connectTimeoutMs);
    upstream.onerror = () => { if (phase !== "ready") reject("Home Assistant nicht erreichbar."); };
    upstream.onclose = () => close(phase === "ready" ? 1011 : 1008);
    upstream.onmessage = (ev) => {
      let msg;
      try { msg = JSON.parse(typeof ev.data === "string" ? ev.data : String(ev.data)); } catch { return; }
      if (!isObject(msg)) return;
      if (phase === "connecting") {
        if (msg.type === "auth_required") upstream.send(JSON.stringify({ type: "auth", access_token: token }));
        else if (msg.type === "auth_ok") { clearTimeout(timer); phase = "ready"; send({ type: "auth_ok" }); }
        else if (msg.type === "auth_invalid") reject("Home Assistant hat den Zugang abgelehnt.");
        return;
      }
      const kind = pending.get(msg.id);
      if (msg.type === "result" && kind) {
        pending.delete(msg.id);
        clearTimeout(writeTimers.get(msg.id));
        writeTimers.delete(msg.id);
        if (msg.success !== true) {
          // nur ein schlichter Fehlercode, nie der Text von Home Assistant (Pfade, Namen, Nutzerdaten); ein Schreibbefehl nur „abgelehnt“
          if (kind === "call_service") return send(refuse(msg.id, "ha_error", "Home Assistant hat die Anfrage abgelehnt."));
          const code = isObject(msg.error) && typeof msg.error.code === "string" && /^[a-z_]{1,40}$/.test(msg.error.code) ? msg.error.code : "unknown_error";
          return send(refuse(msg.id, code, "Home Assistant hat die Anfrage abgelehnt."));
        }
        if (kind === "get_states") {
          const states = cleanStates(msg.result);
          if (writable) { entities.clear(); for (const st of states) remember(st); }
          return send({ id: msg.id, type: "result", success: true, result: states });
        }
        if (kind === "call_service") return send({ id: msg.id, type: "result", success: true, result: null }); // weder context noch response von Home Assistant
        if (kind === "subscribe_events") { subscription = msg.id; return send({ id: msg.id, type: "result", success: true, result: null }); }
      } else if (msg.type === "pong" && kind === "ping") {
        pending.delete(msg.id);
        send({ id: msg.id, type: "pong" });
      } else if (msg.type === "event" && subscription !== null && msg.id === subscription) {
        const event = cleanEvent(msg.event);
        if (event) {
          if (writable) { if (event.data.new_state) remember(event.data.new_state); else entities.delete(event.data.entity_id); }
          send({ id: msg.id, type: "event", event });
        }
      }
      // alles andere (nie angefragt) fällt weg
    };
  }

  function onBrowserMessage({ opcode, payload }) {
    if (closed) return;
    missedPongs = 0; // jede Nachricht des Browsers zeigt, dass er lebt
    if (opcode === 0x8) return close(1000);
    if (opcode === 0x9) return void (!socket.destroyed && socket.write(encodeFrame(0xa, payload)));
    if (opcode === 0xa) return;
    if (opcode !== 0x1 || payload.length > MAX_BROWSER_MESSAGE) return close(opcode === 0x1 ? 1009 : 1003);
    let msg;
    try { msg = JSON.parse(payload.toString("utf8")); } catch { return close(1007); }
    if (phase === "auth") {
      if (!isObject(msg) || msg.type !== "auth") return reject("Erst anmelden.");
      clearTimeout(timer);
      return connectUpstream(); // access_token des Browsers wird nicht gelesen
    }
    if (phase !== "ready") return close(1008);
    const r = checkRequest(msg, session);
    if (r.close) return close(1008);
    if (r.reply) return send(r.reply);
    if (pending.size >= MAX_PENDING) return send(refuse(r.forward.id, "busy", "Zu viele offene Anfragen."));
    pending.set(r.forward.id, r.forward.type);
    if (r.forward.type === "call_service") {
      const wid = r.forward.id;
      writeTimers.set(wid, setTimeout(() => { writeTimers.delete(wid); if (pending.delete(wid)) send(refuse(wid, "timeout", "Home Assistant antwortet nicht.")); }, writeTimeoutMs).unref());
    }
    try { upstream.send(JSON.stringify(r.forward)); } catch { close(1011); }
  }

  const parser = new FrameParser({ maxPayload: MAX_BROWSER_MESSAGE, onMessage: onBrowserMessage });
  const feed = (chunk) => {
    try { parser.push(chunk); } catch (e) { close(typeof e.code === "number" ? e.code : 1002); }
  };
  socket.on("data", feed);
  socket.on("error", () => close(1006));
  socket.on("close", () => { closed = true; clearTimeout(timer); clearInterval(heartbeat); stopTimers(); try { upstream?.close(); } catch { /* schon zu */ } });
  send({ type: "auth_required" });
  if (head?.length) feed(head);
  return () => close(1001);
}
