// Test des Schreibwegs im Relay (Stufe 2, P3b): node --test homeassistant/heyclima/rootfs/app/ha.control.test.mjs
// Der Browser ist das globale WebSocket von Node, „Home Assistant“ eine Nachbildung mit eigenem WebSocket-Server.
// Geprüft wird vor allem, was NICHT durchgeht: der Proxy schreibt mit dem Supervisor-Token, die Allowlist ist die einzige Sperre.
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createProxy } from "./proxy.mjs";
import { acceptHandshake, checkRequest, createWindow, encodeFrame, FrameParser, WRITE_SERVICES } from "./ha.mjs";

process.env.HA_ALLOW_LOOPBACK = "1"; // nur in Tests: sonst geht der Token nur an ws://supervisor
const TOKEN = "SUPERVISOR-GEHEIM-TOKEN-4711";
const sockets = new Set();
const servers = []; // alle Server des Tests: nach einem fehlgeschlagenen Test schließt after sie, sonst hinge der Lauf
const listen = (srv) => new Promise((r) => {
  servers.push(srv);
  srv.on("connection", (s) => { sockets.add(s); s.on("close", () => sockets.delete(s)); });
  srv.listen(0, "127.0.0.1", () => r(srv.address().port));
});

const WOHN = {
  entity_id: "climate.wohnzimmer", state: "heat", last_updated: "2026-11-11T10:00:01+00:00",
  attributes: {
    friendly_name: "Wohnzimmer", hvac_modes: ["off", "heat", "cool", "auto", "dry", "fan_only"], temperature: 21, current_temperature: 20.4, min_temp: 16, max_temp: 30,
    fan_mode: "auto", fan_modes: ["auto", "low", "high"], swing_mode: "off", swing_modes: ["off", "vertical", "both"], target_temp_high: 25, target_temp_low: 18,
  },
  context: { id: "c1", parent_id: null, user_id: "USER-ID-GEHEIM" },
};
const SCHLAF = {
  entity_id: "climate.schlafzimmer", state: "cool", last_updated: "2026-11-11T10:00:02+00:00",
  attributes: { friendly_name: "Schlafzimmer", hvac_modes: ["off", "cool", "heat_cool"], temperature: 24, min_temp: 18, max_temp: 30 },
};
const NOMIN = { entity_id: "climate.ohne_bereich", state: "heat", attributes: { hvac_modes: ["off", "heat"], temperature: 20 } };
const LIGHT = { entity_id: "light.flur", state: "on", attributes: {}, context: { id: "c3" } };
const SCRIPT = { entity_id: "script.alles_aus", state: "off", attributes: {} };

/** Nachbildung von Home Assistant: auth, get_states, subscribe_events, ping, call_service (mit Fehler und Schweigen auf Wunsch) */
function mockHa() {
  const ha = { received: [], subscribers: new Set(), failCalls: false, silentCalls: false, held: [] };
  const server = http.createServer((req, res) => { res.writeHead(404); res.end(); });
  server.on("upgrade", (req, socket) => {
    if (!acceptHandshake(req, socket)) return;
    const send = (o) => socket.write(encodeFrame(0x1, Buffer.from(JSON.stringify(o))));
    const parser = new FrameParser({
      maxPayload: 1 << 20,
      onMessage: ({ opcode, payload }) => {
        if (opcode === 0x8) return socket.end();
        if (opcode !== 0x1) return;
        const msg = JSON.parse(payload.toString());
        ha.received.push(msg);
        if (msg.type === "auth") return send(msg.access_token === TOKEN ? { type: "auth_ok" } : { type: "auth_invalid" });
        if (msg.type === "get_states") return send({ id: msg.id, type: "result", success: true, result: [WOHN, SCHLAF, NOMIN, LIGHT, SCRIPT] });
        if (msg.type === "subscribe_events") { ha.subscribers.add({ id: msg.id, send }); return send({ id: msg.id, type: "result", success: true, result: null }); }
        if (msg.type === "ping") return send({ id: msg.id, type: "pong" });
        if (msg.type === "call_service") {
          if (ha.silentCalls) return void ha.held.push({ id: msg.id, send });
          if (ha.failCalls) return send({ id: msg.id, type: "result", success: false, error: { code: "service_validation_error", message: `Fehler in /config/secrets.yaml: Token ${TOKEN}` } });
          return send({ id: msg.id, type: "result", success: true, result: { context: { id: "ctx", parent_id: null, user_id: "SUPERVISOR-USER-GEHEIM" }, response: { geheim: 1 } } });
        }
        send({ id: msg.id, type: "result", success: true, result: { done: true } });
      },
    });
    socket.on("data", (d) => parser.push(d));
    socket.on("close", () => { for (const s of ha.subscribers) if (s.send === send) ha.subscribers.delete(s); });
    socket.on("error", () => undefined);
    send({ type: "auth_required" });
  });
  ha.server = server;
  ha.emit = (event) => { for (const s of ha.subscribers) s.send({ id: s.id, type: "event", event }); };
  ha.writes = () => ha.received.filter((m) => m.type === "call_service");
  return ha;
}

function browser(url) {
  const c = { all: [], queue: [], waiters: [], closed: null };
  const ws = new WebSocket(url);
  c.ws = ws;
  c.opened = new Promise((r) => { ws.onopen = () => r(true); ws.onerror = () => r(false); });
  c.closedP = new Promise((r) => { ws.onclose = (e) => { c.closed = e.code; r(e.code); }; });
  ws.onmessage = (ev) => {
    const msg = JSON.parse(ev.data);
    c.all.push(msg);
    const w = c.waiters.find((x) => x.match(msg));
    if (w) { c.waiters.splice(c.waiters.indexOf(w), 1); w.resolve(msg); } else c.queue.push(msg);
  };
  c.next = (match = () => true, ms = 3000) => new Promise((resolve, reject) => {
    const i = c.queue.findIndex(match);
    if (i >= 0) return resolve(c.queue.splice(i, 1)[0]);
    const timer = setTimeout(() => reject(new Error("Zeitüberschreitung beim Warten auf eine Nachricht")), ms);
    c.waiters.push({ match, resolve: (m) => { clearTimeout(timer); resolve(m); } });
  });
  c.send = (o) => ws.send(JSON.stringify(o));
  c.id = 0;
  /** schickt die Anfrage mit der nächsten id und wartet auf die Antwort */
  c.ask = async (o) => { const id = ++c.id; c.send({ id, ...o }); return c.next((m) => m.id === id); };
  return c;
}
const everything = []; // alles, was je ein Browser bekam: Token, user_id und Text von Home Assistant dürfen nirgends stehen

/** Browser, angemeldet, mit der Liste der Entitäten (die der Relay sich dabei merkt) */
async function session(url, { states = true } = {}) {
  const c = browser(url);
  everything.push(c.all);
  assert.equal(await c.opened, true);
  await c.next((m) => m.type === "auth_required");
  c.send({ type: "auth", access_token: "token-des-browsers" });
  assert.equal((await c.next((m) => m.type === "auth_ok" || m.type === "auth_invalid")).type, "auth_ok");
  if (states) assert.equal((await c.ask({ type: "get_states" })).success, true);
  return c;
}
const call = (service, entity_id, service_data, extra = {}) => ({ type: "call_service", domain: "climate", service, ...(service_data === undefined ? {} : { service_data }), target: { entity_id }, ...extra });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let ha, proxy, proxyPort, readonlyProxy, readonlyPort, webDir;
const url = (port) => `ws://127.0.0.1:${port}/ha/ws`;
const proxyFor = (extra = {}) => createProxy({ serverUrl: "http://127.0.0.1:9", webDir, allowedIps: ["127.0.0.1"], mode: "homeassistant", supervisorToken: TOKEN, haWsUrl: `ws://127.0.0.1:${ha.port}/core/websocket`, ...extra });

before(async () => {
  webDir = fs.mkdtempSync(path.join(os.tmpdir(), "hc-hac-"));
  fs.writeFileSync(path.join(webDir, "index.html"), "<html><head></head><body>HeyClima</body></html>");
  ha = mockHa();
  ha.port = await listen(ha.server);
  proxy = proxyFor({ control: true, modeGapMs: 0, rateLimits: { perSecond: 1000, perMinute: 100000 } }); // die Ratenbegrenzung prüft ein eigener Test
  proxyPort = await listen(proxy);
  readonlyProxy = proxyFor({}); // Vorgabe: control aus
  readonlyPort = await listen(readonlyProxy);
});
after(() => {
  fs.rmSync(webDir, { recursive: true, force: true });
  for (const s of sockets) s.destroy();
  for (const srv of servers) srv.close();
});

test("erlaubte Dienste kommen bei Home Assistant verändert-neu aufgebaut an, die Antwort an den Browser ist nur ok", async () => {
  const c = await session(url(proxyPort));
  const cases = [
    ["set_hvac_mode", "climate.wohnzimmer", { hvac_mode: "cool" }],
    ["set_hvac_mode", "climate.wohnzimmer", { hvac_mode: "off" }],
    ["set_hvac_mode", "climate.schlafzimmer", { hvac_mode: "heat_cool" }],
    ["set_temperature", "climate.wohnzimmer", { temperature: 22.5 }],
    ["set_temperature", "climate.wohnzimmer", { temperature: 16 }],
    ["set_temperature", "climate.wohnzimmer", { temperature: 30 }],
    ["set_fan_mode", "climate.wohnzimmer", { fan_mode: "low" }],
    ["set_swing_mode", "climate.wohnzimmer", { swing_mode: "both" }],
  ];
  for (const [service, entity, data] of cases) {
    ha.received.length = 0;
    const id = c.id + 1;
    const r = await c.ask(call(service, entity, data));
    assert.deepEqual(r, { id, type: "result", success: true, result: null }, `${service} ${JSON.stringify(data)}`);
    assert.deepEqual(ha.writes(), [{ id, type: "call_service", domain: "climate", service, service_data: data, target: { entity_id: entity } }], "genau diese Anfrage, nichts sonst");
  }
  c.ws.close();
});

test("zusätzliche Felder der Anfrage führen zur Abweisung (nichts davon geht an Home Assistant)", async () => {
  const c = await session(url(proxyPort));
  ha.received.length = 0;
  const ok = { type: "call_service", domain: "climate", service: "set_temperature", service_data: { temperature: 21 }, target: { entity_id: "climate.wohnzimmer" } };
  const bad = [
    { ...ok, return_response: true },
    { ...ok, context: { user_id: "x" } },
    { ...ok, area_id: "wohnzimmer" },
    { ...ok, extra: 1 },
    { ...ok, target: { entity_id: "climate.wohnzimmer", area_id: "alle" } },
    { ...ok, target: { entity_id: "climate.wohnzimmer", device_id: "d1" } },
    { ...ok, target: { entity_id: "climate.wohnzimmer", label_id: "l1" } },
    { ...ok, target: { area_id: "wohnzimmer" } },
    { ...ok, target: { device_id: "d1" } },
    { ...ok, target: { floor_id: "f" } },
    { ...ok, target: {} },
    { ...ok, target: null },
    { ...ok, target: "climate.wohnzimmer" },
    { ...ok, target: [{ entity_id: "climate.wohnzimmer" }] },
    { ...ok, target: undefined }, // fehlt ganz
    { ...ok, service_data: { temperature: 21, hvac_mode: "heat" } },
    { ...ok, service_data: { temperature: 21, entity_id: "all" } },
    { ...ok, service_data: { temperature: 21, target_temp_high: 25 } },
    { ...ok, service_data: { temperature: 21, target_temp_low: 18 } },
    { ...ok, service_data: { target_temp_high: 25, target_temp_low: 18 } },
    { ...ok, service_data: { entity_id: "climate.wohnzimmer" } },
    { ...ok, service_data: {} },
    { ...ok, service_data: undefined },
    { ...ok, service_data: null },
    { ...ok, service_data: [21] },
    { ...ok, service_data: 21 },
    { ...ok, service_data: { hvac_mode: "heat" } },
    { type: "call_service", domain: "climate", service: "set_hvac_mode", service_data: { temperature: 21 }, target: { entity_id: "climate.wohnzimmer" } },
    { type: "call_service", domain: "climate", service: "set_fan_mode", service_data: { fan_mode: "low", swing_mode: "off" }, target: { entity_id: "climate.wohnzimmer" } },
  ];
  for (const b of bad) {
    const r = await c.ask(b);
    assert.equal(r.success, false, JSON.stringify(b));
    assert.ok(["invalid_request", "unknown_entity", "not_allowed"].includes(r.error.code), `${JSON.stringify(b)} -> ${r.error.code}`);
  }
  // __proto__ als Schlüssel (JSON.parse legt ihn als eigenen Schlüssel an)
  const id = ++c.id;
  c.ws.send(`{"id":${id},"type":"call_service","domain":"climate","service":"set_temperature","service_data":{"temperature":21,"__proto__":{"x":1}},"target":{"entity_id":"climate.wohnzimmer"}}`);
  assert.equal((await c.next((m) => m.id === id)).success, false);
  assert.deepEqual(ha.writes(), []);
  c.ws.close();
});

test("alle anderen Dienste und Domänen werden abgewiesen, auch bekannte Entitäten, und gehen nie an Home Assistant", async () => {
  const c = await session(url(proxyPort));
  ha.received.length = 0;
  const w = "climate.wohnzimmer";
  const bad = [
    { domain: "homeassistant", service: "restart" },
    { domain: "homeassistant", service: "stop" },
    { domain: "homeassistant", service: "turn_off", target: { entity_id: w } },
    { domain: "homeassistant", service: "update_entity", target: { entity_id: w } },
    { domain: "script", service: "alles_aus" },
    { domain: "script", service: "turn_on", target: { entity_id: "script.alles_aus" } },
    { domain: "automation", service: "trigger", target: { entity_id: "automation.x" } },
    { domain: "light", service: "turn_on", target: { entity_id: "light.flur" } },
    { domain: "shell_command", service: "boese" },
    { domain: "hassio", service: "addon_stop", service_data: { addon: "core_ssh" } },
    { domain: "persistent_notification", service: "create", service_data: { message: "x" } },
    { domain: "rest_command", service: "x" },
    { domain: "python_script", service: "x" },
    { domain: "lock", service: "unlock", target: { entity_id: "lock.haustuer" } },
    { domain: "Climate", service: "set_temperature", service_data: { temperature: 21 }, target: { entity_id: w } },
    { domain: "climate ", service: "set_temperature", service_data: { temperature: 21 }, target: { entity_id: w } },
    { domain: "climate", service: "set_preset_mode", service_data: { preset_mode: "eco" }, target: { entity_id: w } },
    { domain: "climate", service: "set_humidity", service_data: { humidity: 40 }, target: { entity_id: w } },
    { domain: "climate", service: "set_aux_heat", service_data: { aux_heat: true }, target: { entity_id: w } },
    { domain: "climate", service: "set_swing_horizontal_mode", service_data: { swing_horizontal_mode: "off" }, target: { entity_id: w } },
    { domain: "climate", service: "turn_on", target: { entity_id: w } },
    { domain: "climate", service: "turn_off", target: { entity_id: w } },
    { domain: "climate", service: "toggle", target: { entity_id: w } },
    { domain: "climate", service: "reload" },
    { domain: "climate", service: "constructor" },
    { domain: "climate", service: "__proto__" },
    { domain: "climate", service: "toString" },
    { domain: "climate", service: ["set_temperature"], service_data: { temperature: 21 }, target: { entity_id: w } },
    { domain: ["climate"], service: "set_temperature", service_data: { temperature: 21 }, target: { entity_id: w } },
    { domain: "climate", service: 5 },
    { domain: "climate" },
    { service: "set_temperature", service_data: { temperature: 21 }, target: { entity_id: w } },
    {},
  ];
  for (const b of bad) {
    const r = await c.ask({ type: "call_service", ...b });
    assert.equal(r.success, false, JSON.stringify(b));
    assert.equal(r.error.code, "not_allowed", JSON.stringify(b));
  }
  // andere Arten von Anfragen, die schreiben könnten
  for (const b of [{ type: "execute_script", sequence: [{ service: "homeassistant.restart" }] }, { type: "render_template", template: "{{ 1 }}" }, { type: "fire_event", event_type: "x" }, { type: "config/entity_registry/update", entity_id: w }, { type: "supervisor/api", endpoint: "/core/restart", method: "post" }]) {
    const r = await c.ask(b);
    assert.equal(r.error.code, "not_allowed", JSON.stringify(b));
  }
  assert.deepEqual(ha.writes(), []);
  assert.deepEqual(ha.received.map((m) => m.type), [], "bei Home Assistant kam gar nichts an");
  c.ws.close();
});

test("fremde, nie gemeldete und vieldeutige Entitäten werden abgewiesen", async () => {
  const c = await session(url(proxyPort));
  ha.received.length = 0;
  const bad = [
    "light.flur", "script.alles_aus", "person.gast", // nicht climate: nie gemeldet
    "climate.nie_gemeldet", "climate.wohnzimmer2", "climate.wohnzimmer ", " climate.wohnzimmer", "CLIMATE.wohnzimmer", "climate.Wohnzimmer",
    "all", "none", "climate.*", "climate.%", "climate.", "climate", "*", "", ".", "climate.wohnzimmer,climate.schlafzimmer", "climate.wohnzimmer, climate.schlafzimmer",
    "climate.wohnzimmer\n", "climate.wohnzimmer\u0000", "climate.wohnzimmer.x", "climate/wohnzimmer", "climate.__proto__", "climate.constructor", "light.flur,climate.wohnzimmer",
  ];
  for (const entity of bad) {
    for (const [service, data] of [["set_temperature", { temperature: 21 }], ["set_hvac_mode", { hvac_mode: "off" }]]) {
      const r = await c.ask(call(service, entity, data));
      assert.equal(r.success, false, JSON.stringify(entity));
      assert.ok(["unknown_entity", "invalid_request"].includes(r.error.code), `${JSON.stringify(entity)} -> ${r.error.code}`);
    }
  }
  for (const entity of [["climate.wohnzimmer"], { entity_id: "climate.wohnzimmer" }, 5, null, true, ["climate.wohnzimmer", "climate.schlafzimmer"], []]) {
    const r = await c.ask(call("set_temperature", entity, { temperature: 21 }));
    assert.equal(r.success, false, JSON.stringify(entity));
  }
  assert.deepEqual(ha.writes(), []);
  c.ws.close();
});

test("Entitäten gelten erst, nachdem sie dem Browser gemeldet wurden: nicht vor get_states, mit Ereignis dazu, mit Entfernen wieder weg", async () => {
  const c = await session(url(proxyPort), { states: false });
  ha.received.length = 0;
  // noch keine Liste: nichts ist bekannt
  let r = await c.ask(call("set_temperature", "climate.wohnzimmer", { temperature: 21 }));
  assert.equal(r.error.code, "unknown_entity");
  assert.deepEqual(ha.writes(), []);
  assert.equal((await c.ask({ type: "subscribe_events", event_type: "state_changed" })).success, true);
  // ein climate-Ereignis macht die Entität bekannt, ein fremdes nicht
  ha.emit({ event_type: "state_changed", data: { entity_id: "climate.neu", new_state: { entity_id: "climate.neu", state: "heat", attributes: { hvac_modes: ["off", "heat"], min_temp: 10, max_temp: 20 } } } });
  ha.emit({ event_type: "state_changed", data: { entity_id: "light.neu", new_state: { entity_id: "light.neu", state: "on", attributes: {} } } });
  await c.next((m) => m.type === "event");
  r = await c.ask(call("set_temperature", "climate.neu", { temperature: 15 }));
  assert.equal(r.success, true);
  assert.equal((await c.ask(call("set_temperature", "climate.neu", { temperature: 25 }))).error.code, "invalid_request", "Bereich der Entität gilt, nicht der einer anderen");
  assert.equal((await c.ask(call("set_temperature", "light.neu", { temperature: 15 }))).success, false);
  // Entfernen (new_state null): nicht mehr steuerbar
  ha.emit({ event_type: "state_changed", data: { entity_id: "climate.neu", new_state: null } });
  await c.next((m) => m.type === "event");
  assert.equal((await c.ask(call("set_temperature", "climate.neu", { temperature: 15 }))).error.code, "unknown_entity");
  // eine neue Liste ersetzt die alte
  assert.equal((await c.ask({ type: "get_states" })).success, true);
  assert.equal((await c.ask(call("set_temperature", "climate.wohnzimmer", { temperature: 21 }))).success, true);
  // eine andere Verbindung weiß davon nichts
  const d = await session(url(proxyPort), { states: false });
  assert.equal((await d.ask(call("set_temperature", "climate.wohnzimmer", { temperature: 21 }))).error.code, "unknown_entity");
  c.ws.close(); d.ws.close();
});

test("Werte: Typ, Bereich der Entität, endliche Zahlen, Liste der Entität", async () => {
  const c = await session(url(proxyPort));
  ha.received.length = 0;
  const bad = [
    // Betriebsart
    ["set_hvac_mode", "climate.wohnzimmer", { hvac_mode: "heat_cool" }], // die Entität kennt es nicht
    ["set_hvac_mode", "climate.schlafzimmer", { hvac_mode: "heat" }],
    ["set_hvac_mode", "climate.wohnzimmer", { hvac_mode: "HEAT" }],
    ["set_hvac_mode", "climate.wohnzimmer", { hvac_mode: "boost" }],
    ["set_hvac_mode", "climate.wohnzimmer", { hvac_mode: "" }],
    ["set_hvac_mode", "climate.wohnzimmer", { hvac_mode: ["heat"] }],
    ["set_hvac_mode", "climate.wohnzimmer", { hvac_mode: null }],
    ["set_hvac_mode", "climate.wohnzimmer", { hvac_mode: 1 }],
    ["set_hvac_mode", "climate.wohnzimmer", { hvac_mode: { a: 1 } }],
    ["set_hvac_mode", "climate.ohne_bereich", { hvac_mode: "cool" }],
    // Temperatur
    ["set_temperature", "climate.wohnzimmer", { temperature: 15.9 }],
    ["set_temperature", "climate.wohnzimmer", { temperature: 30.1 }],
    ["set_temperature", "climate.wohnzimmer", { temperature: 100 }],
    ["set_temperature", "climate.wohnzimmer", { temperature: -5 }],
    ["set_temperature", "climate.wohnzimmer", { temperature: 0 }],
    ["set_temperature", "climate.schlafzimmer", { temperature: 17 }],
    ["set_temperature", "climate.wohnzimmer", { temperature: "21" }],
    ["set_temperature", "climate.wohnzimmer", { temperature: "" }],
    ["set_temperature", "climate.wohnzimmer", { temperature: null }],
    ["set_temperature", "climate.wohnzimmer", { temperature: true }],
    ["set_temperature", "climate.wohnzimmer", { temperature: [21] }],
    ["set_temperature", "climate.wohnzimmer", { temperature: { v: 21 } }],
    ["set_temperature", "climate.ohne_bereich", { temperature: 20 }], // ohne min_temp/max_temp wird nicht geschrieben
    // Lüfter und Schwenken: nur Einträge der Liste der Entität
    ["set_fan_mode", "climate.wohnzimmer", { fan_mode: "turbo" }],
    ["set_fan_mode", "climate.wohnzimmer", { fan_mode: "LOW" }],
    ["set_fan_mode", "climate.wohnzimmer", { fan_mode: "" }],
    ["set_fan_mode", "climate.wohnzimmer", { fan_mode: ["low"] }],
    ["set_fan_mode", "climate.wohnzimmer", { fan_mode: 1 }],
    ["set_fan_mode", "climate.schlafzimmer", { fan_mode: "low" }], // diese Entität hat keine Liste
    ["set_swing_mode", "climate.wohnzimmer", { swing_mode: "horizontal" }],
    ["set_swing_mode", "climate.wohnzimmer", { swing_mode: null }],
    ["set_swing_mode", "climate.schlafzimmer", { swing_mode: "off" }],
  ];
  for (const [service, entity, data] of bad) {
    const r = await c.ask(call(service, entity, data));
    assert.equal(r.success, false, `${service} ${entity} ${JSON.stringify(data)}`);
    assert.equal(r.error.code, "invalid_request", `${service} ${JSON.stringify(data)}`);
  }
  // NaN und Infinity gibt es in JSON nicht, aber 1e999 wird beim Lesen zu Infinity
  for (const v of ["1e999", "-1e999"]) {
    const id = ++c.id;
    c.ws.send(`{"id":${id},"type":"call_service","domain":"climate","service":"set_temperature","service_data":{"temperature":${v}},"target":{"entity_id":"climate.wohnzimmer"}}`);
    const r = await c.next((m) => m.id === id);
    assert.equal(r.error.code, "invalid_request", v);
  }
  assert.deepEqual(ha.writes(), [], "nichts davon erreichte Home Assistant");
  c.ws.close();
});

test("checkRequest: NaN und Infinity (nur aus Code möglich) werden abgewiesen; ohne control wird nie geschrieben", () => {
  const entities = new Map([["climate.a", { hvac_modes: ["off", "heat"], min_temp: 10, max_temp: 30, fan_modes: ["a"], swing_modes: ["b"] }]]);
  const mk = (over = {}) => ({ lastId: 0, subscribed: false, control: true, entities, ...over });
  const msg = (data, service = "set_temperature") => ({ id: 1, type: "call_service", domain: "climate", service, service_data: data, target: { entity_id: "climate.a" } });
  for (const v of [NaN, Infinity, -Infinity]) {
    const r = checkRequest(msg({ temperature: v }), mk());
    assert.equal(r.forward, undefined, String(v));
    assert.equal(r.reply.error.code, "invalid_request");
  }
  assert.ok(checkRequest(msg({ temperature: 20 }), mk()).forward);
  assert.equal(checkRequest(msg({ temperature: 20 }), mk({ control: false })).reply.error.code, "not_allowed");
  assert.equal(checkRequest(msg({ temperature: 20 }), mk({ control: undefined })).reply.error.code, "not_allowed");
  assert.equal(checkRequest(msg({ temperature: 20 }), { lastId: 0, subscribed: false }).reply.error.code, "not_allowed", "Sitzung ohne control: lesend");
  assert.deepEqual(Object.keys(WRITE_SERVICES).sort(), ["set_fan_mode", "set_hvac_mode", "set_swing_mode", "set_temperature"]);
  assert.ok(Object.isFrozen(WRITE_SERVICES));
});

test("control aus (Vorgabe): jede Schreibanfrage wird abgewiesen, auch eine erlaubte, nichts geht an Home Assistant", async () => {
  const c = await session(url(readonlyPort));
  ha.received.length = 0;
  for (const [service, entity, data] of [
    ["set_hvac_mode", "climate.wohnzimmer", { hvac_mode: "cool" }],
    ["set_temperature", "climate.wohnzimmer", { temperature: 21 }],
    ["set_fan_mode", "climate.wohnzimmer", { fan_mode: "low" }],
    ["set_swing_mode", "climate.wohnzimmer", { swing_mode: "off" }],
  ]) {
    const r = await c.ask(call(service, entity, data));
    assert.equal(r.success, false);
    assert.equal(r.error.code, "not_allowed");
  }
  assert.deepEqual(ha.writes(), []);
  // Lesen geht weiter
  assert.equal((await c.ask({ type: "get_states" })).success, true);
  c.ws.close();
});

test("Ratenbegrenzung: je Verbindung 5 je Sekunde, insgesamt 60 je Minute; Abgewiesenes zählt nicht", async () => {
  const p = proxyFor({ control: true }); // Vorgaben
  const port = await listen(p);
  const c = await session(url(port));
  ha.received.length = 0;
  const codes = [];
  // ungültige Anfragen verbrauchen kein Kontingent
  for (let i = 0; i < 10; i++) assert.equal((await c.ask(call("set_temperature", "climate.wohnzimmer", { temperature: 99 }))).error.code, "invalid_request");
  for (let i = 0; i < 7; i++) codes.push((await c.ask(call("set_temperature", "climate.wohnzimmer", { temperature: 20 }))).error?.code ?? "ok");
  assert.deepEqual(codes, ["ok", "ok", "ok", "ok", "ok", "rate_limited", "rate_limited"]);
  assert.equal(ha.writes().length, 5, "nur die erlaubten erreichen Home Assistant");
  await sleep(1100);
  assert.equal((await c.ask(call("set_temperature", "climate.wohnzimmer", { temperature: 20 }))).success, true, "nach einer Sekunde wieder frei");
  c.ws.close();
});

test("Ratenbegrenzung insgesamt: gilt für alle Verbindungen zusammen", async () => {
  const p = proxyFor({ control: true, rateLimits: { perSecond: 5, perMinute: 7 } });
  const port = await listen(p);
  const a = await session(url(port));
  const b = await session(url(port));
  ha.received.length = 0;
  for (let i = 0; i < 4; i++) assert.equal((await a.ask(call("set_temperature", "climate.wohnzimmer", { temperature: 20 }))).success, true);
  for (let i = 0; i < 3; i++) assert.equal((await b.ask(call("set_temperature", "climate.wohnzimmer", { temperature: 20 }))).success, true);
  assert.equal((await a.ask(call("set_temperature", "climate.wohnzimmer", { temperature: 20 }))).error.code, "rate_limited");
  await sleep(1100); // die Sekundengrenze ist wieder frei, die Minutengrenze nicht
  assert.equal((await b.ask(call("set_temperature", "climate.wohnzimmer", { temperature: 20 }))).error.code, "rate_limited");
  assert.equal(ha.writes().length, 7);
  // Lesen ist davon nicht betroffen
  assert.equal((await b.ask({ type: "get_states" })).success, true);
  a.ws.close(); b.ws.close();
});

test("Home Assistant lehnt ab: nur ein fester Fehlercode beim Browser, weder Text noch Token noch Nutzerkennung", async () => {
  const c = await session(url(proxyPort));
  ha.failCalls = true;
  const r = await c.ask(call("set_temperature", "climate.wohnzimmer", { temperature: 21 }));
  ha.failCalls = false;
  assert.deepEqual(r, { id: c.id, type: "result", success: false, error: { code: "ha_error", message: "Home Assistant hat die Anfrage abgelehnt." } });
  // der Erfolg trägt weder context noch response von Home Assistant
  const ok = await c.ask(call("set_temperature", "climate.wohnzimmer", { temperature: 21 }));
  assert.deepEqual(ok, { id: c.id, type: "result", success: true, result: null });
  c.ws.close();
});

test("Home Assistant antwortet nicht: Zeitüberschreitung, der Platz wird frei, eine späte Antwort kommt nicht mehr durch", async () => {
  const p = proxyFor({ control: true, writeTimeoutMs: 300, rateLimits: { perSecond: 100, perMinute: 1000 } });
  const port = await listen(p);
  const c = await session(url(port));
  ha.silentCalls = true;
  ha.held.length = 0;
  const r = await c.ask(call("set_temperature", "climate.wohnzimmer", { temperature: 21 }));
  ha.silentCalls = false;
  assert.equal(r.error.code, "timeout");
  assert.equal((await c.ask({ type: "ping" })).type, "pong");
  ha.held[0].send({ id: ha.held[0].id, type: "result", success: true, result: { context: { user_id: "SUPERVISOR-USER-GEHEIM" } } });
  await sleep(200);
  assert.equal(c.all.filter((m) => m.id === ha.held[0].id).length, 1, "nur die Zeitüberschreitung, keine zweite Antwort");
  c.ws.close();
});

test("kein unbeantworteter Schreibbefehl belegt mehr als 8 Plätze", async () => {
  const p = proxyFor({ control: true, writeTimeoutMs: 5000, rateLimits: { perSecond: 100, perMinute: 1000 } });
  const port = await listen(p);
  const c = await session(url(port));
  ha.silentCalls = true;
  for (let i = 0; i < 8; i++) c.send({ id: ++c.id, ...call("set_temperature", "climate.wohnzimmer", { temperature: 21 }) });
  const r = await c.ask(call("set_temperature", "climate.wohnzimmer", { temperature: 21 }));
  ha.silentCalls = false;
  assert.equal(r.error.code, "busy");
  c.ws.close();
});

test("index.html meldet control (nur im Modus homeassistant), Modus heyclima hat keinen Schreibweg", async () => {
  const get = (port) => new Promise((resolve) => http.get({ host: "127.0.0.1", port, path: "/", headers: { "x-ingress-path": "/api/hassio_ingress/abc" } }, (res) => { let b = ""; res.on("data", (d) => (b += d)); res.on("end", () => resolve(b)); }));
  assert.match(await get(proxyPort), /window\.__HEYCLIMA_CONTROL__=true;/);
  assert.match(await get(readonlyPort), /window\.__HEYCLIMA_CONTROL__=false;/);
  const hey = createProxy({ serverUrl: "http://127.0.0.1:9", webDir, allowedIps: ["127.0.0.1"], mode: "heyclima", control: true });
  const heyPort = await listen(hey);
  assert.match(await get(heyPort), /window\.__HEYCLIMA_CONTROL__=false;/, "ohne Modus homeassistant gibt es kein control");
});

test("Token, Nutzerkennung und Text von Home Assistant stehen in keiner Nachricht an irgendeinen Browser", () => {
  const raw = JSON.stringify(everything);
  assert.ok(raw.length > 1000);
  for (const geheim of [TOKEN, "SUPERVISOR-USER-GEHEIM", "USER-ID-GEHEIM", "secrets.yaml", "service_validation_error", "geheim\":1", "context"]) assert.ok(!raw.includes(geheim), geheim);
});

test("busy wird vor dem Verbrauch des Kontingents geprüft: eine abgewiesene Anfrage kostet nichts", () => {
  const entities = new Map([["climate.a", { hvac_modes: ["off", "heat"], min_temp: 10, max_temp: 30 }]]);
  const writes = createWindow(5, 1000);
  const total = createWindow(60, 60_000);
  const msg = (id) => ({ id, type: "call_service", domain: "climate", service: "set_temperature", service_data: { temperature: 20 }, target: { entity_id: "climate.a" } });
  let full = true;
  const session = { lastId: 0, subscribed: false, control: true, entities, writes, total, busy: () => full };
  const r = checkRequest(msg(1), session, 100);
  assert.equal(r.reply.error.code, "busy");
  assert.equal(writes.has(100) && total.has(100), true);
  for (let i = 0; i < 5; i++) assert.equal(writes.has(100), true, "nichts verbraucht");
  full = false;
  for (let i = 2; i <= 6; i++) assert.ok(checkRequest(msg(i), session, 100).forward, String(i)); // alle 5 des Kontingents noch da
  assert.equal(checkRequest(msg(7), session, 100).reply.error.code, "rate_limited");
});

test("Betriebsart je Entität höchstens alle 3 s (Schutz des Verdichters), gemeinsam für alle Verbindungen; andere Dienste und Entitäten nicht betroffen", async () => {
  const p = proxyFor({ control: true, modeGapMs: 400, rateLimits: { perSecond: 100, perMinute: 1000 } });
  const port = await listen(p);
  const a = await session(url(port));
  const b = await session(url(port));
  ha.received.length = 0;
  assert.equal((await a.ask(call("set_hvac_mode", "climate.wohnzimmer", { hvac_mode: "cool" }))).success, true);
  assert.equal((await a.ask(call("set_hvac_mode", "climate.wohnzimmer", { hvac_mode: "heat" }))).error.code, "rate_limited");
  assert.equal((await b.ask(call("set_hvac_mode", "climate.wohnzimmer", { hvac_mode: "heat" }))).error.code, "rate_limited", "auch von einer anderen Verbindung");
  assert.equal((await a.ask(call("set_hvac_mode", "climate.schlafzimmer", { hvac_mode: "cool" }))).success, true, "andere Entität");
  assert.equal((await a.ask(call("set_temperature", "climate.wohnzimmer", { temperature: 22 }))).success, true, "anderer Dienst");
  assert.equal((await a.ask(call("set_hvac_mode", "climate.wohnzimmer", { hvac_mode: "boost" }))).error.code, "invalid_request", "Ungültiges setzt nichts zurück");
  assert.equal(ha.writes().length, 3);
  await sleep(450);
  assert.equal((await b.ask(call("set_hvac_mode", "climate.wohnzimmer", { hvac_mode: "heat" }))).success, true, "nach dem Abstand wieder frei");
  assert.equal((await a.ask(call("set_hvac_mode", "climate.wohnzimmer", { hvac_mode: "off" }))).error.code, "rate_limited", "die abgewiesene Anfrage hat den Abstand nicht verlängert, die erlaubte ihn neu gesetzt");
  a.ws.close(); b.ws.close();
});

test("Vorgabe: 3 Sekunden Abstand bei der Betriebsart", async () => {
  const p = proxyFor({ control: true, rateLimits: { perSecond: 100, perMinute: 1000 } });
  const port = await listen(p);
  const c = await session(url(port));
  assert.equal((await c.ask(call("set_hvac_mode", "climate.wohnzimmer", { hvac_mode: "cool" }))).success, true);
  await sleep(1200); // länger als 1 Sekunde, kürzer als 3
  assert.equal((await c.ask(call("set_hvac_mode", "climate.wohnzimmer", { hvac_mode: "heat" }))).error.code, "rate_limited");
  c.ws.close();
});
