// Test des lesenden Relays zu Home Assistant (Stufe 2): node --test homeassistant/heyclima/rootfs/app/ha.test.mjs
// Der Browser ist das globale WebSocket von Node, „Home Assistant“ eine Nachbildung mit eigenem WebSocket-Server.
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import net from "node:net";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import crypto from "node:crypto";
import { createProxy } from "./proxy.mjs";
import { acceptHandshake, checkUpstreamUrl, cleanEvent, cleanState, cleanStates, checkRequest, encodeFrame, FrameParser, isAllowedEntity } from "./ha.mjs";

process.env.HA_ALLOW_LOOPBACK = "1"; // nur in Tests: sonst geht der Token nur an ws://supervisor
const TOKEN = "SUPERVISOR-GEHEIM-TOKEN-4711";
const sockets = new Set();
const listen = (srv) => new Promise((r) => {
  srv.on("connection", (s) => { sockets.add(s); s.on("close", () => sockets.delete(s)); });
  srv.listen(0, "127.0.0.1", () => r(srv.address().port));
});

const CLIMATE = {
  entity_id: "climate.wohnzimmer", state: "heat", last_changed: "2026-11-11T10:00:00+00:00", last_updated: "2026-11-11T10:00:01+00:00",
  attributes: { friendly_name: "Wohnzimmer", hvac_modes: ["off", "heat", "cool"], temperature: 21, current_temperature: 20.4, min_temp: 16, max_temp: 30, entity_picture: "/local/geheim.png", access_token: "nicht-weitergeben" },
  context: { id: "c1", parent_id: null, user_id: "USER-ID-GEHEIM" },
};
const PERSON = { entity_id: "person.gast", state: "home", attributes: { friendly_name: "Gast", latitude: 52.5, longitude: 13.4 }, context: { id: "c2", user_id: "u" } };
const LIGHT = { entity_id: "light.flur", state: "on", attributes: {}, context: { id: "c3" } };

/** Nachbildung von Home Assistant: WebSocket-Server mit auth, get_states, subscribe_events, ping */
function mockHa({ rejectAuth = false, failStates = false, bigStates = 0 } = {}) {
  const ha = { received: [], subscribers: new Set(), clients: new Set() };
  const server = http.createServer((req, res) => { res.writeHead(404); res.end(); });
  server.on("upgrade", (req, socket) => {
    if (!acceptHandshake(req, socket)) return;
    const send = (o) => socket.write(encodeFrame(0x1, Buffer.from(JSON.stringify(o))));
    const state = { id: null };
    const parser = new FrameParser({
      maxPayload: 1 << 20,
      onMessage: ({ opcode, payload }) => {
        if (opcode === 0x8) return socket.end();
        if (opcode !== 0x1) return;
        const msg = JSON.parse(payload.toString());
        ha.received.push(msg);
        if (msg.type === "auth") return msg.access_token === TOKEN && !rejectAuth ? send({ type: "auth_ok", ha_version: "2026.11.0" }) : send({ type: "auth_invalid", message: `falscher Token ${msg.access_token}` });
        if (msg.type === "get_states" && failStates) return send({ id: msg.id, type: "result", success: false, error: { code: "komisch! /config/secrets.yaml", message: "Fehler in /config/secrets.yaml: password=geheim" } });
        if (msg.type === "get_states" && bigStates) return send({ id: msg.id, type: "result", success: true, result: Array.from({ length: bigStates }, (_, i) => ({ ...CLIMATE, entity_id: `climate.gross_${i}`, attributes: { friendly_name: "x".repeat(50_000) } })) });
        if (msg.type === "get_states") return send({ id: msg.id, type: "result", success: true, result: [CLIMATE, PERSON, LIGHT, { entity_id: "climate.kaputt", state: 5, attributes: null }] });
        if (msg.type === "subscribe_events") { state.id = msg.id; ha.subscribers.add({ id: msg.id, send }); return send({ id: msg.id, type: "result", success: true, result: null }); }
        if (msg.type === "ping") return send({ id: msg.id, type: "pong" });
        send({ id: msg.id, type: "result", success: true, result: { done: true } }); // z. B. call_service würde „klappen“
      },
    });
    ha.clients.add(socket);
    socket.on("data", (d) => parser.push(d));
    socket.on("close", () => ha.clients.delete(socket));
    socket.on("error", () => undefined);
    send({ type: "auth_required", ha_version: "2026.11.0" });
  });
  ha.server = server;
  ha.emit = (event) => { for (const s of ha.subscribers) s.send({ id: s.id, type: "event", event }); };
  return ha;
}

/** Browser: WebSocket mit Nachrichtenliste (all); next(match) liefert die erste noch nicht abgeholte passende Nachricht */
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
  return c;
}
const login = async (c) => {
  assert.equal(await c.opened, true);
  assert.deepEqual(await c.next((m) => m.type === "auth_required"), { type: "auth_required" });
  c.send({ type: "auth", access_token: "token-des-browsers" });
  return c.next((m) => m.type === "auth_ok" || m.type === "auth_invalid");
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const everything = []; // alles, was je ein Browser bekam: der Token darf nirgends stehen

let ha, proxy, proxyPort, webDir;
const wsUrl = () => `ws://127.0.0.1:${proxyPort}/ha/ws`;
const open = () => { const c = browser(wsUrl()); everything.push(c.all); return c; };

before(async () => {
  webDir = fs.mkdtempSync(path.join(os.tmpdir(), "hc-ha-"));
  fs.writeFileSync(path.join(webDir, "index.html"), "<html><head></head><body>HeyClima</body></html>");
  ha = mockHa();
  const haPort = ha.port = await listen(ha.server);
  proxy = createProxy({ serverUrl: "http://127.0.0.1:9", webDir, allowedIps: ["127.0.0.1"], mode: "homeassistant", supervisorToken: TOKEN, haWsUrl: `ws://127.0.0.1:${haPort}/core/websocket` });
  proxyPort = await listen(proxy);
});
after(() => {
  fs.rmSync(webDir, { recursive: true, force: true });
  for (const s of sockets) s.destroy();
  proxy.close(); ha.server.close();
});

test("Anmeldung: der Proxy meldet sich mit dem Supervisor-Token an, der Token des Browsers geht nicht weiter", async () => {
  ha.received.length = 0;
  const c = open();
  assert.deepEqual(await login(c), { type: "auth_ok" });
  assert.deepEqual(ha.received, [{ type: "auth", access_token: TOKEN }]);
  c.ws.close();
  await c.closedP;
});

test("get_states: nur climate, nur bekannte Attribute, ohne context", async () => {
  const c = open();
  await login(c);
  c.send({ id: 1, type: "get_states" });
  const r = await c.next((m) => m.id === 1);
  assert.equal(r.success, true);
  assert.equal(r.result.length, 2);
  assert.equal(r.result[0].entity_id, "climate.wohnzimmer");
  assert.deepEqual(r.result[0].attributes, { friendly_name: "Wohnzimmer", hvac_modes: ["off", "heat", "cool"], temperature: 21, current_temperature: 20.4, min_temp: 16, max_temp: 30 });
  assert.deepEqual(r.result[1], { entity_id: "climate.kaputt", state: "unknown", attributes: {} });
  assert.deepEqual(Object.keys(r.result[0]).sort(), ["attributes", "entity_id", "last_updated", "state"], "last_changed geht nicht mit");
  const raw = JSON.stringify(r);
  for (const geheim of ["person.gast", "light.flur", "USER-ID-GEHEIM", "geheim.png", "nicht-weitergeben", "latitude", "context"]) assert.ok(!raw.includes(geheim), geheim);
  c.ws.close();
});

test("subscribe_events: state_changed von climate kommt an (ohne context und old_state), anderes nicht", async () => {
  const c = open();
  await login(c);
  c.send({ id: 1, type: "subscribe_events", event_type: "state_changed" });
  assert.deepEqual(await c.next((m) => m.id === 1), { id: 1, type: "result", success: true, result: null });
  ha.emit({ event_type: "state_changed", data: { entity_id: "light.flur", new_state: LIGHT, old_state: LIGHT }, context: { user_id: "x" } });
  ha.emit({ event_type: "call_service", data: { domain: "climate" } });
  ha.emit({ event_type: "state_changed", data: { entity_id: "climate.wohnzimmer", new_state: CLIMATE, old_state: { ...CLIMATE, state: "off" } }, origin: "LOCAL", time_fired: "2026-11-11T10:00:02+00:00", context: { user_id: "USER-ID-GEHEIM" } });
  ha.emit({ event_type: "state_changed", data: { entity_id: "climate.weg", new_state: null, old_state: CLIMATE } });
  const e1 = await c.next((m) => m.type === "event");
  assert.equal(e1.event.data.entity_id, "climate.wohnzimmer");
  assert.equal(e1.event.data.new_state.state, "heat");
  assert.equal(e1.event.data.new_state.attributes.temperature, 21);
  assert.deepEqual(Object.keys(e1.event).sort(), ["data", "event_type"], "time_fired geht nicht mit");
  assert.deepEqual(Object.keys(e1.event.data.new_state).sort(), ["attributes", "entity_id", "last_updated", "state"]);
  assert.deepEqual(Object.keys(e1.event.data).sort(), ["entity_id", "new_state"]);
  const e2 = await c.next((m) => m.type === "event" && m.event.data.entity_id === "climate.weg");
  assert.equal(e2.event.data.new_state, null);
  await sleep(100);
  assert.equal(c.all.filter((m) => m.type === "event").length, 2, "genau zwei Ereignisse (Licht und call_service blieben zurück)");
  assert.ok(!JSON.stringify(c.all).includes("USER-ID-GEHEIM"));
  c.ws.close();
});

test("ping wird beantwortet", async () => {
  const c = open();
  await login(c);
  c.send({ id: 5, type: "ping" });
  assert.deepEqual(await c.next((m) => m.id === 5), { id: 5, type: "pong" });
  c.ws.close();
});

test("call_service und alle anderen Arten werden abgewiesen und nie an Home Assistant weitergegeben", async () => {
  ha.received.length = 0;
  const c = open();
  await login(c);
  let id = 1;
  const nots = [
    { type: "call_service", domain: "climate", service: "set_temperature", service_data: { temperature: 30 }, target: { entity_id: "climate.wohnzimmer" } },
    { type: "execute_script", sequence: [] },
    { type: "render_template", template: "{{ states }}" },
    { type: "config/auth/list" },
    { type: "get_config" },
    { type: "auth", access_token: "zweite-anmeldung" },
    { type: "unsubscribe_events", subscription: 1 },
    { type: "subscribe_events", event_type: "call_service" },
    { type: "subscribe_events" },
    { type: "subscribe_events", event_type: "*" },
    { type: "subscribe_trigger", trigger: { platform: "state", entity_id: "light.flur" } },
    { type: "supervisor/api", endpoint: "/addons" },
    {},
  ];
  for (const n of nots) {
    const msg = { id: id++, ...n };
    c.send(msg);
    const r = await c.next((m) => m.id === msg.id);
    assert.equal(r.success, false, JSON.stringify(n));
    assert.equal(r.error.code, "not_allowed");
  }
  assert.deepEqual(ha.received.map((m) => m.type), ["auth"], "bei Home Assistant kam nur die Anmeldung des Proxys an");
  // das erlaubte Abonnement geht, ein zweites nicht
  c.send({ id: id++, type: "subscribe_events", event_type: "state_changed" });
  assert.equal((await c.next((m) => m.id === id - 1)).success, true);
  c.send({ id: id++, type: "subscribe_events", event_type: "state_changed" });
  assert.equal((await c.next((m) => m.id === id - 1)).success, false);
  assert.deepEqual(ha.received.map((m) => m.type), ["auth", "subscribe_events"]);
  assert.deepEqual(ha.received[1], { id: 14, type: "subscribe_events", event_type: "state_changed" });
  c.ws.close();
});

test("zusätzliche Felder einer erlaubten Anfrage gehen nicht mit", async () => {
  ha.received.length = 0;
  const c = open();
  await login(c);
  c.send({ id: 1, type: "get_states", extra: "x", domain: "light" });
  await c.next((m) => m.id === 1);
  assert.deepEqual(ha.received.at(-1), { id: 1, type: "get_states" });
  c.ws.close();
});

test("Vor der Anmeldung und mit ungültigen Nachrichten: schließen", async () => {
  let c = open();
  assert.equal(await c.opened, true);
  await c.next((m) => m.type === "auth_required");
  c.send({ id: 1, type: "get_states" });
  assert.equal((await c.next((m) => m.type === "auth_invalid")).type, "auth_invalid");
  assert.equal(await c.closedP, 1008);

  c = open(); await login(c);
  c.send({ id: 3, type: "ping" }); await c.next((m) => m.id === 3);
  c.send({ id: 3, type: "ping" }); // gleiche id: Protokollverstoß
  assert.equal(await c.closedP, 1008);

  c = open(); await login(c);
  c.ws.send("das ist kein JSON");
  assert.equal(await c.closedP, 1007);

  c = open(); await login(c);
  c.ws.send(Buffer.from([1, 2, 3]));
  assert.equal(await c.closedP, 1003);

  c = open(); await login(c);
  c.ws.send(JSON.stringify({ id: 1, type: "ping", pad: "x".repeat(10_000) }));
  assert.equal(await c.closedP, 1009);
});

test("Token erreicht den Browser nie (auch nicht bei Fehlern von Home Assistant)", async () => {
  // Home Assistant lehnt den Token ab und schreibt ihn sogar in seine Meldung: der Browser sieht nur den Standardtext
  const bad = mockHa({ rejectAuth: true });
  const port = await listen(bad.server);
  const p2 = createProxy({ serverUrl: "http://127.0.0.1:9", webDir, allowedIps: ["127.0.0.1"], mode: "homeassistant", supervisorToken: TOKEN, haWsUrl: `ws://127.0.0.1:${port}/` });
  const p2Port = await listen(p2);
  const c = browser(`ws://127.0.0.1:${p2Port}/ha/ws`);
  everything.push(c.all);
  const r = await login(c);
  assert.equal(r.type, "auth_invalid");
  assert.ok(!JSON.stringify(r).includes(TOKEN));
  await c.closedP;
  p2.close(); bad.server.close();

  // Home Assistant nicht erreichbar
  const dead = http.createServer(); const deadPort = await listen(dead); dead.close(); await sleep(50);
  const p3 = createProxy({ serverUrl: "http://127.0.0.1:9", webDir, allowedIps: ["127.0.0.1"], mode: "homeassistant", supervisorToken: TOKEN, haWsUrl: `ws://127.0.0.1:${deadPort}/core/websocket` });
  const p3Port = await listen(p3);
  const d = browser(`ws://127.0.0.1:${p3Port}/ha/ws`);
  everything.push(d.all);
  const r2 = await login(d);
  assert.equal(r2.type, "auth_invalid");
  assert.ok(!JSON.stringify(r2).includes(String(deadPort)), "keine Adresse in der Meldung");
  p3.close();

  // ganz ohne Token (homeassistant_api fehlt)
  const p4 = createProxy({ serverUrl: "http://127.0.0.1:9", webDir, allowedIps: ["127.0.0.1"], mode: "homeassistant", supervisorToken: "", haWsUrl: "ws://127.0.0.1:9/" });
  const p4Port = await listen(p4);
  const e = browser(`ws://127.0.0.1:${p4Port}/ha/ws`);
  everything.push(e.all);
  assert.equal((await login(e)).type, "auth_invalid");
  p4.close();

  await sleep(50);
  assert.ok(everything.length >= 8);
  for (const list of everything) assert.ok(!JSON.stringify(list).includes(TOKEN), "Token in einer Nachricht an den Browser");
});

test("nur Verbindungen vom Supervisor-Ingress (Quelladresse)", async () => {
  const p = createProxy({ serverUrl: "http://127.0.0.1:9", webDir, allowedIps: ["172.30.32.2"], mode: "homeassistant", supervisorToken: TOKEN, haWsUrl: "ws://127.0.0.1:9/" });
  const port = await listen(p);
  const c = browser(`ws://127.0.0.1:${port}/ha/ws`);
  assert.equal(await c.opened, false);
  const r = await new Promise((resolve) => http.get({ host: "127.0.0.1", port, path: "/", agent: false }, (res) => resolve(res.statusCode)));
  assert.equal(r, 403);
  p.close();
});

test("Modus homeassistant: kein /api, kein /api/events, index.html kennt den Modus; Modus heyclima hat kein /ha/ws", async () => {
  const get = (port, p) => new Promise((resolve, reject) => http.get({ host: "127.0.0.1", port, path: p, agent: false }, (res) => {
    const chunks = []; res.on("data", (d) => chunks.push(d)); res.on("end", () => resolve({ status: res.statusCode, body: Buffer.concat(chunks).toString() }));
  }).on("error", reject));
  assert.equal((await get(proxyPort, "/api/sticks")).status, 404);
  assert.equal((await get(proxyPort, "/ha/ws")).status, 426);
  const index = await get(proxyPort, "/");
  assert.match(index.body, /window\.__HEYCLIMA_MODE__="homeassistant"/);
  const events = new WebSocket(`ws://127.0.0.1:${proxyPort}/api/events`);
  assert.equal(await new Promise((r) => { events.onopen = () => r(true); events.onerror = () => r(false); }), false);

  const normal = createProxy({ serverUrl: "http://127.0.0.1:9", webDir, allowedIps: ["127.0.0.1"], supervisorToken: TOKEN });
  const port = await listen(normal);
  assert.match((await get(port, "/")).body, /window\.__HEYCLIMA_MODE__="heyclima"/);
  assert.equal((await get(port, "/ha/ws")).status, 200, "ohne Modus homeassistant ist /ha/ws nur ein App-Pfad (index.html)");
  const c = browser(`ws://127.0.0.1:${port}/ha/ws`);
  assert.equal(await c.opened, false, "kein WebSocket unter /ha/ws im Modus heyclima");
  normal.close();
  assert.throws(() => createProxy({ serverUrl: "http://127.0.0.1:9", webDir, mode: "egal" }), /mode/);
});

test("höchstens 16 gleichzeitige Verbindungen", async () => {
  await sleep(300); // Verbindungen der Tests davor sind sicher geschlossen
  const cs = [];
  for (let i = 0; i < 16; i++) { const c = open(); await login(c); cs.push(c); }
  const extra = browser(wsUrl());
  assert.equal(await extra.opened, false);
  cs[0].ws.close(); await cs[0].closedP; await sleep(100);
  const again = open();
  assert.equal((await login(again)).type, "auth_ok");
  for (const c of [...cs, again]) c.ws.close();
  await Promise.all([...cs, again].map((c) => c.closedP));
  await sleep(100);
});

/** Browser von Hand: Handshake, danach schweigt er (liest und antwortet nicht) */
const rawClient = (port, { send } = {}) => new Promise((resolve, reject) => {
  const s = net.connect(port, "127.0.0.1", () => s.write(`GET /ha/ws HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n`));
  const c = { s, data: [], closed: false };
  s.on("data", (d) => { c.data.push(d); if (c.data.length === 1) { if (send) send(s); resolve(c); } });
  s.on("close", () => { c.closed = true; });
  s.on("error", () => { c.closed = true; });
  setTimeout(() => reject(new Error("kein Handshake")), 2000);
});

test("Anmeldung mit Zeitlimit: Verbindungen ohne auth blockieren den Platz nicht dauerhaft", async () => {
  const p = createProxy({ serverUrl: "http://127.0.0.1:9", webDir, allowedIps: ["127.0.0.1"], mode: "homeassistant", supervisorToken: TOKEN, haWsUrl: `ws://127.0.0.1:${ha.port}/`, maxHaSockets: 4, authTimeoutMs: 250 });
  const port = await listen(p);
  const idle = [];
  for (let i = 0; i < 4; i++) idle.push(await rawClient(port));
  const fifth = browser(`ws://127.0.0.1:${port}/ha/ws`);
  assert.equal(await fifth.opened, false, "solange die vier warten, ist kein Platz frei");
  await sleep(600);
  assert.ok(idle.every((c) => c.closed), "ohne auth nach dem Zeitlimit geschlossen");
  const again = browser(`ws://127.0.0.1:${port}/ha/ws`);
  assert.equal((await login(again)).type, "auth_ok", "danach ist wieder Platz");
  again.ws.close();
  p.close();
});

test("kein Pong auf zwei Herzschläge: Verbindung wird geschlossen; ein Browser, der antwortet, bleibt", async () => {
  const p = createProxy({ serverUrl: "http://127.0.0.1:9", webDir, allowedIps: ["127.0.0.1"], mode: "homeassistant", supervisorToken: TOKEN, haWsUrl: `ws://127.0.0.1:${ha.port}/`, pingMs: 60 });
  const port = await listen(p);
  const mask = (o) => encodeFrame(0x1, Buffer.from(JSON.stringify(o)), true);
  const dead = await rawClient(port, { send: (s) => s.write(mask({ type: "auth" })) }); // meldet sich an, antwortet nie auf Pings
  const alive = browser(`ws://127.0.0.1:${port}/ha/ws`); // das WebSocket von Node antwortet selbst
  await login(alive);
  await sleep(500);
  assert.equal(dead.closed, true, "stumm: geschlossen");
  assert.equal(alive.closed, null, "antwortet: bleibt offen");
  alive.send({ id: 1, type: "ping" });
  assert.deepEqual(await alive.next((m) => m.id === 1), { id: 1, type: "pong" });
  alive.ws.close();
  p.close();
});

test("Fehlertexte von Home Assistant gehen nicht an den Browser", async () => {
  const bad = mockHa({ failStates: true });
  const port = await listen(bad.server);
  const p = createProxy({ serverUrl: "http://127.0.0.1:9", webDir, allowedIps: ["127.0.0.1"], mode: "homeassistant", supervisorToken: TOKEN, haWsUrl: `ws://127.0.0.1:${port}/` });
  const pPort = await listen(p);
  const c = browser(`ws://127.0.0.1:${pPort}/ha/ws`);
  await login(c);
  c.send({ id: 1, type: "get_states" });
  const r = await c.next((m) => m.id === 1);
  assert.equal(r.success, false);
  assert.deepEqual(r.error, { code: "unknown_error", message: "Home Assistant hat die Anfrage abgelehnt." });
  assert.ok(!JSON.stringify(c.all).includes("secrets"));
  c.ws.close(); p.close(); bad.server.close();
});

test("ein Browser, der nicht liest, wird geschlossen statt den Speicher zu füllen", async () => {
  const big = mockHa({ bigStates: 100 }); // je Antwort rund 5 MB
  const port = await listen(big.server);
  const p = createProxy({ serverUrl: "http://127.0.0.1:9", webDir, allowedIps: ["127.0.0.1"], mode: "homeassistant", supervisorToken: TOKEN, haWsUrl: `ws://127.0.0.1:${port}/` });
  const pPort = await listen(p);
  const mask = (o) => encodeFrame(0x1, Buffer.from(JSON.stringify(o)), true);
  const c = await rawClient(pPort, { send: (s) => { s.write(mask({ type: "auth" })); } });
  c.s.pause();
  await sleep(200);
  for (let i = 1; i <= 8; i++) c.s.write(mask({ id: i, type: "get_states" }));
  for (let i = 0; i < 50 && !c.closed; i++) { c.s.resume(); c.s.pause(); await sleep(100); }
  // Verbindung zu Home Assistant ist zu, sobald der Relay aufgibt
  for (let i = 0; i < 30 && big.clients.size; i++) await sleep(100);
  assert.equal(big.clients.size, 0, "Relay hat die Verbindung zu Home Assistant geschlossen");
  c.s.destroy(); p.close(); big.server.close();
});

test("schließt Home Assistant, schließt der Relay den Browser", async () => {
  const c = open();
  await login(c);
  for (const s of ha.clients) s.destroy();
  assert.equal(await c.closedP, 1011);
});

test("Filterfunktionen", () => {
  assert.equal(isAllowedEntity("climate.a_b1"), true);
  for (const bad of ["light.x", "climate", "climate.", "Climate.x", "climate.x y", "sensor.temp", 5, null]) assert.equal(isAllowedEntity(bad), false, String(bad));
  assert.equal(cleanState({ entity_id: "light.x", state: "on" }), null);
  assert.equal(cleanState(null), null);
  assert.deepEqual(cleanStates("kein Array"), []);
  assert.equal(cleanEvent({ event_type: "state_changed", data: { entity_id: "climate.a", new_state: { entity_id: "light.b", state: "on" } } }), null);
  assert.equal(cleanEvent({ event_type: "call_service", data: { entity_id: "climate.a" } }), null);
  const s = { id: 0 };
  assert.deepEqual(checkRequest({ id: 1, type: "ping" }, s), { forward: { id: 1, type: "ping" } });
  assert.ok(checkRequest({ id: 1, type: "ping" }, s).close, "id muss wachsen");
  assert.ok(checkRequest({ id: "2", type: "ping" }, { lastId: 0 }).close);
  assert.ok(checkRequest("x", { lastId: 0 }).close);
});

test("der Token geht nur zum Supervisor (oder Loopback), nie zu einem anderen Host", () => {
  assert.equal(checkUpstreamUrl("ws://supervisor/core/websocket", {}), "ws://supervisor/core/websocket");
  assert.throws(() => checkUpstreamUrl("ws://127.0.0.1:8123/", {}), /HA_WS_URL/, "Loopback nur mit Testschalter");
  assert.throws(() => checkUpstreamUrl("ws://localhost/", { HA_ALLOW_LOOPBACK: "0" }), /HA_WS_URL/);
  assert.equal(checkUpstreamUrl("ws://127.0.0.1:8123/", { HA_ALLOW_LOOPBACK: "1" }), "ws://127.0.0.1:8123/");
  for (const bad of ["ws://evil.example/core/websocket", "wss://supervisor/core/websocket", "http://supervisor/", "ws://supervisor.evil.example/x", "ws://user@evil.example/", "kein url"]) {
    assert.throws(() => checkUpstreamUrl(bad, { HA_ALLOW_LOOPBACK: "1" }), /HA_WS_URL/, bad);
  }
  assert.throws(() => createProxy({ serverUrl: "http://127.0.0.1:9", webDir, mode: "homeassistant", supervisorToken: TOKEN, haWsUrl: "ws://evil.example/" }), /HA_WS_URL/);
});

test("höchstens 8 unbeantwortete Anfragen je Verbindung", async () => {
  const slow = mockHa();
  slow.server.removeAllListeners("upgrade");
  const port = await listen(slow.server);
  // Home Assistant, das nie auf get_states antwortet
  slow.server.on("upgrade", (req, socket) => {
    acceptHandshake(req, socket);
    const parser = new FrameParser({ maxPayload: 1 << 20, onMessage: ({ opcode, payload }) => {
      if (opcode !== 0x1) return;
      const m = JSON.parse(payload.toString());
      if (m.type === "auth") socket.write(encodeFrame(0x1, Buffer.from(JSON.stringify({ type: "auth_ok" }))));
    } });
    socket.on("data", (d) => parser.push(d)); socket.on("error", () => undefined);
    socket.write(encodeFrame(0x1, Buffer.from(JSON.stringify({ type: "auth_required" }))));
  });
  const p = createProxy({ serverUrl: "http://127.0.0.1:9", webDir, allowedIps: ["127.0.0.1"], mode: "homeassistant", supervisorToken: TOKEN, haWsUrl: `ws://127.0.0.1:${port}/` });
  const pPort = await listen(p);
  const c = browser(`ws://127.0.0.1:${pPort}/ha/ws`);
  await login(c);
  for (let i = 1; i <= 9; i++) c.send({ id: i, type: "get_states" });
  const busy = await c.next((m) => m.id === 9);
  assert.equal(busy.error.code, "busy");
  c.ws.close(); p.close(); slow.server.close();
});

test("Rahmen: Längen, Fragmente, Steuerrahmen, Maske", () => {
  const got = [];
  const parser = (opts = {}) => new FrameParser({ maxPayload: 70000, onMessage: (m) => got.push(m), ...opts });
  for (const n of [0, 5, 125, 126, 300, 65535, 65536]) {
    got.length = 0;
    const p = parser();
    const payload = crypto.randomBytes(n);
    const frame = encodeFrame(0x2, payload, true);
    p.push(frame.subarray(0, 1)); p.push(frame.subarray(1, 3)); p.push(frame.subarray(3)); // in Stücken
    assert.equal(got.length, 1, `Länge ${n}`);
    assert.deepEqual(got[0].payload, payload);
  }
  // Fragmente mit Ping dazwischen
  got.length = 0;
  const p = parser();
  const a = encodeFrame(0x1, Buffer.from("Hal"), true); a[0] &= 0x7f; // kein FIN
  const b = encodeFrame(0x0, Buffer.from("lo"), true);
  p.push(Buffer.concat([a, encodeFrame(0x9, Buffer.from("p"), true), b]));
  assert.deepEqual(got.map((m) => [m.opcode, m.payload.toString()]), [[0x9, "p"], [0x1, "Hallo"]]);
  // Fehler: unmaskiert vom Client, RSV, zu groß, Fortsetzung ohne Anfang, fragmentierter Steuerrahmen
  const code = (frame, opts) => { try { parser(opts).push(frame); } catch (e) { return e.code; } return null; };
  assert.equal(code(encodeFrame(0x1, Buffer.from("x"), false)), 1002);
  const rsv = encodeFrame(0x1, Buffer.from("x"), true); rsv[0] |= 0x40;
  assert.equal(code(rsv), 1002);
  assert.equal(code(encodeFrame(0x1, Buffer.alloc(200), true), { maxPayload: 100 }), 1009);
  const huge = Buffer.alloc(10); huge[0] = 0x82; huge[1] = 0xff; huge.writeBigUInt64BE(1n << 40n, 2);
  assert.equal(code(Buffer.concat([huge, Buffer.alloc(4)])), 1009);
  assert.equal(code(encodeFrame(0x0, Buffer.from("x"), true)), 1002);
  const frag = encodeFrame(0x9, Buffer.from("x"), true); frag[0] &= 0x7f;
  assert.equal(code(frag), 1002);
  assert.equal(code(encodeFrame(0x3, Buffer.from("x"), true)), 1002);
});

test("Handshake: ungültige Aufrufe bekommen 400", async () => {
  const raw = (headers) => new Promise((resolve) => {
    const s = net.connect(proxyPort, "127.0.0.1", () => s.write(`GET /ha/ws HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n${headers}\r\n`));
    let out = ""; s.on("data", (d) => (out += d)); s.on("close", () => resolve(out.split("\r\n")[0]));
    setTimeout(() => s.destroy(), 1500);
  });
  assert.match(await raw("Sec-WebSocket-Version: 13\r\n"), /400/);
  assert.match(await raw("Sec-WebSocket-Version: 8\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"), /400/);
  assert.match(await raw("Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"), /101/);
});
