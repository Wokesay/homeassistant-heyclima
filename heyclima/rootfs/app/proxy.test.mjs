// Test des HeyClima-Proxys: node --test homeassistant/heyclima/rootfs/app/proxy.test.mjs
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import net from "node:net";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { createProxy } from "./proxy.mjs";

const seen = []; // Anfragen, die der falsche Upstream erreichten
let upstream, proxy, evil, webDir, upstreamPort, proxyPort, evilHits = 0;

const sockets = new Set(); // alle Verbindungen merken, damit der Testlauf sauber endet
const listen = (srv) => new Promise((r) => {
  srv.on("connection", (s) => { sockets.add(s); s.on("close", () => sockets.delete(s)); });
  srv.listen(0, "127.0.0.1", () => r(srv.address().port));
});
const get = (p, { headers = {}, method = "GET", body } = {}) =>
  new Promise((resolve, reject) => {
    const req = http.request({ host: "127.0.0.1", port: proxyPort, path: p, method, headers, agent: false }, (res) => {
      const chunks = [];
      res.on("data", (c) => chunks.push(c));
      res.on("end", () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks).toString() }));
    });
    req.on("error", reject);
    req.end(body);
  });

before(async () => {
  webDir = fs.mkdtempSync(path.join(os.tmpdir(), "hc-web-"));
  fs.mkdirSync(path.join(webDir, "_expo"));
  fs.writeFileSync(path.join(webDir, "index.html"), '<html><head><script src="/_expo/app.js"></script><link rel="icon" href="/favicon.ico"></head><body>HeyClima</body></html>');
  fs.writeFileSync(path.join(webDir, "_expo", "app.js"), "console.log(1)");
  fs.writeFileSync(path.join(path.dirname(webDir), "geheim.txt"), "geheim");

  upstream = http.createServer((req, res) => {
    if (req.url === "/api/hsts") { res.writeHead(200, { "Strict-Transport-Security": "max-age=1", "Public-Key-Pins": "x", "Keep-Alive": "timeout=5", "Proxy-Authenticate": "x", "X-Test": "bleibt" }); return res.end("ok"); }
    if (req.url === "/api/redir-own") { res.writeHead(302, { Location: `http://127.0.0.1:${upstreamPort}/api/ziel?a=1` }); return res.end(); }
    if (req.url === "/api/redir-rel") { res.writeHead(302, { Location: "/api/ziel" }); return res.end(); }
    if (req.url === "/api/redir-fremd") { res.writeHead(302, { Location: "http://evil.example/x" }); return res.end(); }
    if (req.url === "/api/haengt") return; // antwortet nie
    if (req.url === "/api/redir-doppel") { res.writeHead(302, { Location: `http://127.0.0.1:${upstreamPort}//evil.example/x` }); return res.end(); }
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      seen.push({ url: req.url, method: req.method, headers: req.headers, body });
      res.writeHead(200, { "Content-Type": "application/json", "Set-Cookie": "sid=1; Path=/; Domain=app.heyclima.com; HttpOnly" });
      res.end(JSON.stringify({ ok: true }));
    });
  });
  upstream.on("upgrade", (req, socket) => {
    seen.push({ url: req.url, upgrade: true, headers: req.headers });
    if (req.url.includes("haengt")) return; // Upgrade wird nie beantwortet
    socket.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: x\r\n\r\n");
    socket.on("data", (d) => socket.write(Buffer.concat([Buffer.from("echo:"), d])));
  });
  upstreamPort = await listen(upstream);
  evil = http.createServer((req, res) => { evilHits++; res.end("böse"); });
  const evilPort = await listen(evil);
  evil.port = evilPort;

  proxy = createProxy({ serverUrl: `http://127.0.0.1:${upstreamPort}`, webDir, allowedIps: ["127.0.0.1"], upstreamTimeoutMs: 300 });
  proxyPort = await listen(proxy);
});

after(() => { fs.rmSync(webDir, { recursive: true, force: true }); fs.rmSync(path.join(path.dirname(webDir), "geheim.txt"), { force: true }); for (const s of sockets) s.destroy(); for (const s of [proxy, upstream, evil]) s.close(); });

test("liefert index.html aus und schreibt Pfade auf den Ingress-Pfad um", async () => {
  const r = await get("/", { headers: { "x-ingress-path": "/api/hassio_ingress/abc_DEF-1" } });
  assert.equal(r.status, 200);
  assert.match(r.body, /src="\/api\/hassio_ingress\/abc_DEF-1\/_expo\/app\.js"/);
  assert.match(r.body, /href="\/api\/hassio_ingress\/abc_DEF-1\/favicon\.ico"/);
  assert.match(r.body, /window\.__HEYCLIMA_BASE__="\/api\/hassio_ingress\/abc_DEF-1"/);
  assert.match(r.headers["cache-control"], /no-store/);
});

test("ungültiger Ingress-Pfad wird nicht eingesetzt", async () => {
  const r = await get("/", { headers: { "x-ingress-path": '/x"><script>alert(1)</script>' } });
  assert.equal(r.status, 200);
  assert.doesNotMatch(r.body, /alert/);
  assert.match(r.body, /__HEYCLIMA_BASE__=""/);
});

test("liefert Dateien aus und fällt bei Pfaden ohne Endung auf index.html zurück", async () => {
  const js = await get("/_expo/app.js");
  assert.equal(js.status, 200);
  assert.equal(js.body, "console.log(1)");
  assert.match(js.headers["content-type"], /javascript/);
  assert.match((await get("/zeitplaene")).body, /HeyClima/);
  assert.equal((await get("/fehlt.js")).status, 404);
});

test("ein Verzeichnis mit Endung ergibt 404, kein Absturz", async () => {
  fs.mkdirSync(path.join(webDir, "ordner.js"));
  assert.equal((await get("/ordner.js")).status, 404);
  assert.equal((await get("/fehlt.png")).status, 404);
});

test("kein Ausbrechen aus dem Web-Ordner", async () => {
  for (const p of ["/../geheim.txt", "/%2e%2e/geheim.txt", "/_expo/..%2f..%2fgeheim.txt", "/%00"]) {
    const r = await get(p);
    assert.notEqual(r.body, "geheim", p);
    assert.ok([400, 404].includes(r.status) || r.body.includes("HeyClima"), p);
  }
});

test("leitet /api an den Server weiter, Host und Origin passend, nur erlaubte Header", async () => {
  seen.length = 0;
  const r = await get("/api/me?x=1", { method: "POST", body: '{"a":1}', headers: {
    host: "homeassistant.local:8123", origin: "http://homeassistant.local:8123", authorization: "Bearer t",
    cookie: "ha_auth=geheim", referer: "http://homeassistant.local:8123/x", "x-remote-user-id": "1", "x-hass-user-id": "2",
    "x-ingress-path": "/api/hassio_ingress/abc", "x-forwarded-for": "1.2.3.4", "accept-language": "de", "user-agent": "t/1",
    "content-type": "application/json" } });
  assert.equal(r.status, 200);
  assert.equal(r.body, '{"ok":true}');
  const s = seen[0];
  assert.equal(s.url, "/api/me?x=1");
  assert.equal(s.method, "POST");
  assert.equal(s.body, '{"a":1}');
  assert.equal(s.headers.host, `127.0.0.1:${upstreamPort}`);
  assert.equal(s.headers.origin, `http://127.0.0.1:${upstreamPort}`);
  assert.equal(s.headers.authorization, "Bearer t");
  assert.equal(s.headers["content-type"], "application/json");
  assert.equal(s.headers["accept-language"], "de");
  assert.equal(s.headers["user-agent"], "t/1");
  for (const k of ["cookie", "referer", "x-remote-user-id", "x-hass-user-id", "x-ingress-path", "x-forwarded-for"]) assert.equal(s.headers[k], undefined, k);
  const c = r.headers["set-cookie"][0];
  assert.doesNotMatch(c, /Domain=/i);
  assert.match(c, /HttpOnly/);
  assert.match(c, /Path=\/api\/hassio_ingress\/abc\//);
});

test("entfernt unerwünschte Antwort-Header (HSTS usw.)", async () => {
  const r = await get("/api/hsts");
  assert.equal(r.status, 200);
  for (const k of ["strict-transport-security", "public-key-pins", "keep-alive", "proxy-authenticate"]) assert.equal(r.headers[k], undefined, k);
  assert.equal(r.headers["x-test"], "bleibt");
});

test("Location: eigener Server wird auf den Ingress-Pfad umgeschrieben, fremde fallen weg", async () => {
  const base = { "x-ingress-path": "/api/hassio_ingress/abc" };
  assert.equal((await get("/api/redir-own", { headers: base })).headers.location, "/api/hassio_ingress/abc/api/ziel?a=1");
  assert.equal((await get("/api/redir-rel", { headers: base })).headers.location, "/api/hassio_ingress/abc/api/ziel");
  assert.equal((await get("/api/redir-fremd", { headers: base })).headers.location, undefined);
});

test("Location ohne Ingress-Pfad wird nie zu //host", async () => {
  assert.equal((await get("/api/redir-own")).headers.location, "/api/ziel?a=1");
  assert.equal((await get("/api/redir-doppel")).headers.location, "/evil.example/x");
  assert.equal((await get("/api/redir-doppel", { headers: { "x-ingress-path": "/api/hassio_ingress/abc" } })).headers.location, "/api/hassio_ingress/abc//evil.example/x");
});

test("502 bei nicht erreichbarem Upstream und bei Zeitüberschreitung", async () => {
  const dead = http.createServer(); const deadPort = await listen(dead); dead.close(); // Port nun zu
  const p = createProxy({ serverUrl: `http://127.0.0.1:${deadPort}`, webDir, allowedIps: ["127.0.0.1"] });
  const port = await listen(p);
  const status = await new Promise((resolve, reject) => http.get({ host: "127.0.0.1", port, path: "/api/x", agent: false }, (r) => { r.resume(); resolve(r.statusCode); }).on("error", reject));
  assert.equal(status, 502);
  p.closeAllConnections(); p.close();
  assert.equal((await get("/api/haengt")).status, 502); // upstreamTimeoutMs: 300
});

test("leitet den WebSocket /api/events weiter", async () => {
  seen.length = 0;
  const sock = net.connect(proxyPort, "127.0.0.1");
  const got = await new Promise((resolve, reject) => {
    let buf = "";
    sock.on("error", reject);
    sock.on("data", (d) => {
      buf += d.toString();
      if (buf.includes("\r\n\r\n") && !buf.includes("echo:")) sock.write("hallo");
      if (buf.includes("echo:hallo")) resolve(buf);
    });
    sock.write("GET /api/events?token=abc HTTP/1.1\r\nHost: homeassistant.local\r\nOrigin: http://homeassistant.local\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n");
  });
  sock.destroy();
  assert.match(got, /^HTTP\/1\.1 101/);
  assert.equal(seen[0].url, "/api/events?token=abc");
  assert.equal(seen[0].headers.host, `127.0.0.1:${upstreamPort}`);
  assert.equal(seen[0].headers.origin, `http://127.0.0.1:${upstreamPort}`);
});

const upgradeRequest = (url) => `GET ${url} HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n`;

test("WebSocket-Upgrade, den der Server nie beantwortet, wird nach dem Zeitlimit geschlossen", async () => {
  const sock = net.connect(proxyPort, "127.0.0.1");
  const t0 = Date.now();
  await new Promise((resolve, reject) => { sock.on("error", () => resolve()); sock.on("close", resolve); sock.write(upgradeRequest("/api/events?haengt=1")); setTimeout(() => reject(new Error("nicht geschlossen")), 3000); });
  assert.ok(Date.now() - t0 < 2000);
  sock.destroy();
});

test("ein aufgebauter WebSocket bleibt länger als das Zeitlimit offen", async () => {
  const sock = net.connect(proxyPort, "127.0.0.1");
  await new Promise((r) => { sock.once("data", r); sock.write(upgradeRequest("/api/events?token=abc")); });
  await new Promise((r) => setTimeout(r, 700)); // upstreamTimeoutMs ist 300
  const got = await new Promise((resolve) => { sock.once("data", (d) => resolve(d.toString())); sock.write("spaet"); });
  assert.match(got, /echo:spaet/);
  sock.destroy();
});

test("Bytes direkt nach dem WebSocket-Upgrade (head) kommen beim Upstream an", async () => {
  const sock = net.connect(proxyPort, "127.0.0.1");
  const got = await new Promise((resolve, reject) => {
    let buf = "";
    sock.on("error", reject);
    sock.on("data", (d) => { buf += d.toString(); if (buf.includes("echo:frueh")) resolve(buf); });
    sock.write("GET /api/events?token=abc HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\nfrueh");
  });
  sock.destroy();
  assert.match(got, /echo:frueh/);
});

test("WebSocket nur auf /api/events", async () => {
  const sock = net.connect(proxyPort, "127.0.0.1");
  const closed = new Promise((r) => { sock.on("close", r); sock.on("error", r); });
  sock.write("GET /mqtt HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n");
  await closed;
  assert.ok(!seen.some((s) => s.url === "/mqtt"));
});

test("kein Weiterleiten an fremde Hosts", async () => {
  evilHits = 0;
  for (const p of [`//127.0.0.1:${evil.port}/api/x`, `http://127.0.0.1:${evil.port}/api/x`, `/api@127.0.0.1:${evil.port}/x`, `/apix`]) {
    seen.length = 0;
    const r = await new Promise((resolve) => {
      const s = net.connect(proxyPort, "127.0.0.1");
      let b = "";
      s.on("data", (d) => (b += d));
      s.on("close", () => resolve(b));
      s.on("error", () => resolve(b));
      s.write(`GET ${p} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n`);
    });
    assert.equal(evilHits, 0, p);
    assert.ok(!seen.some((s) => s.headers?.host?.includes(String(evil.port))), p);
    assert.doesNotMatch(r, /böse/, p);
  }
});

test("nur Anfragen vom Supervisor-Ingress", async () => {
  const strict = createProxy({ serverUrl: `http://127.0.0.1:${upstreamPort}`, webDir }); // Vorgabe 172.30.32.2
  const port = await listen(strict);
  seen.length = 0;
  const status = await new Promise((resolve, reject) =>
    http.get({ host: "127.0.0.1", port, path: "/api/me", agent: false }, (res) => { res.resume(); resolve(res.statusCode); }).on("error", reject));
  assert.equal(status, 403);
  assert.equal(seen.length, 0);
  const sock = net.connect(port, "127.0.0.1");
  const out = await new Promise((resolve) => {
    let b = "";
    sock.on("data", (d) => (b += d));
    sock.on("close", () => resolve(b));
    sock.on("error", () => resolve(b));
    sock.write("GET /api/events HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n");
  });
  assert.doesNotMatch(out, /101/);
  assert.equal(seen.length, 0);
  strict.closeAllConnections(); strict.close();
});

test("IPv4-in-IPv6-Adresse des Supervisors wird erkannt", async () => {
  const { isAllowed } = await import("./proxy.mjs");
  assert.equal(isAllowed("::ffff:172.30.32.2", ["172.30.32.2"]), true);
  assert.equal(isAllowed("172.30.32.3", ["172.30.32.2"]), false);
  assert.equal(isAllowed(undefined, ["172.30.32.2"]), false);
});

test("SIGTERM beendet den Proxy sauber mit Code 0", async () => {
  const child = spawn(process.execPath, [fileURLToPath(new URL("./proxy.mjs", import.meta.url))], { env: { ...process.env, PORT: "0", WEB_DIR: webDir } });
  await new Promise((r) => child.stdout.once("data", r));
  const code = new Promise((r) => child.on("exit", (c) => r(c)));
  child.kill("SIGTERM");
  assert.equal(await code, 0);
});

test("SIGTERM beendet den Proxy auch bei offenem WebSocket", async () => {
  const child = spawn(process.execPath, [fileURLToPath(new URL("./proxy.mjs", import.meta.url))],
    { env: { ...process.env, PORT: "0", WEB_DIR: webDir, SERVER_URL: `http://127.0.0.1:${upstreamPort}`, ALLOWED_IPS: "127.0.0.1" } });
  const port = Number(await new Promise((r) => child.stdout.once("data", (d) => r(/Port (\d+)/.exec(d.toString())?.[1]))));
  assert.ok(port > 0);
  const sock = net.connect(port, "127.0.0.1");
  await new Promise((r) => { sock.once("data", r); sock.write(upgradeRequest("/api/events?token=abc")); }); // WebSocket steht
  const code = new Promise((r) => child.on("exit", (c) => r(c)));
  const timer = setTimeout(() => child.kill("SIGKILL"), 3000);
  child.kill("SIGTERM");
  const c = await code;
  clearTimeout(timer);
  sock.destroy();
  assert.equal(c, 0); // bei SIGKILL wäre der Code null
});
