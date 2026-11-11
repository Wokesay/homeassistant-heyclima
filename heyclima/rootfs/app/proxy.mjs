// HeyClima-Proxy für die Home-Assistant-App (reines Node, keine Abhängigkeiten).
//  - liefert den Web-Export der App aus WEB_DIR aus (index.html wird auf den wechselnden Ingress-Pfad umgeschrieben),
//  - leitet /api/* und den WebSocket /api/events an SERVER_URL weiter (nur dieser Host, kein offener Proxy),
//  - nimmt nur Verbindungen vom Supervisor-Ingress an (Vorgabe 172.30.32.2),
//  - Modus "homeassistant" (Stufe 2): statt /api ein Relay unter /ha/ws zur Home-Assistant-Schnittstelle (ha.mjs), standardmäßig nur
//    lesend; mit control (P3b) zusätzlich ein eng begrenzter Schreibweg für climate-Entitäten.
import http from "node:http";
import https from "node:https";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { acceptHandshake, checkUpstreamUrl, createWindow, relay } from "./ha.mjs";

const SUPERVISOR = "172.30.32.2";
const HA_WS_URL = "ws://supervisor/core/websocket";
const MAX_HA_SOCKETS = 16; // gleichzeitige Browser-Verbindungen zum Relay (je eine eigene zu Home Assistant)
const INGRESS_PATH = /^\/api\/hassio_ingress\/[A-Za-z0-9_-]+$/;
const TYPES = {
  ".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8", ".mjs": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8", ".json": "application/json", ".svg": "image/svg+xml", ".png": "image/png",
  ".jpg": "image/jpeg", ".ico": "image/x-icon", ".ttf": "font/ttf", ".woff": "font/woff", ".woff2": "font/woff2",
  ".map": "application/json", ".txt": "text/plain; charset=utf-8", ".webmanifest": "application/manifest+json",
};
// Nur diese Request-Header gehen an den HeyClima-Server (Allowlist). Cookie (Home-Assistant-Sitzung), x-remote-user-*,
// referer, x-forwarded-* und alles andere von Home Assistant bleibt zurück.
const ALLOW_REQUEST = /^(authorization|content-type|content-length|accept(-.+)?|user-agent|cache-control|pragma|if-.+|range|sec-websocket-.+)$/;
// Antwort-Header, die nicht an Home Assistant (den Browser) gehen: HSTS würde die HA-Adresse festschreiben
const DROP_RESPONSE = /^(strict-transport-security|connection|keep-alive|proxy-.+|public-key-pins(-report-only)?|transfer-encoding)$/;

export function isAllowed(address, allowedIps) {
  if (!address) return false;
  return allowedIps.includes(address.replace(/^::ffff:/i, ""));
}

/** Pfad der Anfrage: nur "/..." (nie "//..." oder eine absolute Adresse), damit der Zielhost nie aus der Anfrage kommt */
function safeTarget(url) {
  if (typeof url !== "string" || !url.startsWith("/") || url.startsWith("//") || /[\s\u0000-\u001f]/.test(url)) return null;
  return url;
}

/** Wert für das Inline-Skript: JSON ohne "<" (kein </script> möglich) */
const js = (v) => JSON.stringify(v).replace(/</g, "\\u003c");

function ingressBase(req) {
  const v = req.headers["x-ingress-path"];
  return typeof v === "string" && INGRESS_PATH.test(v) ? v : "";
}

function upstreamHeaders(req, target) {
  const h = {};
  for (const [k, v] of Object.entries(req.headers)) if (ALLOW_REQUEST.test(k)) h[k] = v;
  h.host = target.host;
  if (req.headers.origin) h.origin = target.origin;
  return h;
}

function rewriteCookie(cookie, base) {
  return cookie.replace(/;\s*Domain=[^;]*/gi, "").replace(/;\s*Path=[^;]*/gi, `; Path=${base || ""}/`);
}

/**
 * mode: "heyclima" (Vorgabe, Stufe 1: die App spricht über /api mit dem HeyClima-Server) oder "homeassistant" (Stufe 2: /ha/ws,
 * /api ist aus). supervisorToken und haWsUrl nur für den Relay (Vorgabe: ws://supervisor/core/websocket); der Token bleibt hier.
 * control (nur Modus homeassistant, Vorgabe false): der Relay lässt die vier climate-Schreibdienste zu (ha.mjs, checkWrite);
 * rateLimits: { perSecond, perMinute }, writeTimeoutMs: Wartezeit auf Home Assistant bei einem Schreibbefehl, modeGapMs: Mindestabstand
 * beim Umschalten der Betriebsart je Entität (Vorgabe 3000, 0 = aus).
 */
export function createProxy({ serverUrl, webDir, allowedIps = [SUPERVISOR], upstreamTimeoutMs = 30_000, mode = "heyclima", supervisorToken = "", haWsUrl = HA_WS_URL, WebSocketImpl, maxHaSockets = MAX_HA_SOCKETS, authTimeoutMs, pingMs, control = false, rateLimits, writeTimeoutMs, modeGapMs = 3000 }) {
  if (mode !== "heyclima" && mode !== "homeassistant") throw new Error("mode muss heyclima oder homeassistant sein");
  const haMode = mode === "homeassistant";
  if (haMode) checkUpstreamUrl(haWsUrl);
  const haControl = haMode && control === true; // nur genau true; jeder andere Wert lässt es aus
  const limits = { perSecond: rateLimits?.perSecond ?? 5, perMinute: rateLimits?.perMinute ?? 60 };
  const totalWindow = createWindow(limits.perMinute, 60_000); // gilt für alle Verbindungen zusammen
  const modeAt = new Map(); // letzte Umschaltung der Betriebsart je Entität, ebenfalls für alle Verbindungen
  const haSockets = new Set();
  const target = new URL(serverUrl);
  if (target.protocol !== "http:" && target.protocol !== "https:") throw new Error("server_url muss mit http:// oder https:// beginnen");
  const lib = target.protocol === "https:" ? https : http;
  const root = path.resolve(webDir);
  const connOptions = { protocol: target.protocol, hostname: target.hostname, port: target.port || (target.protocol === "https:" ? 443 : 80) };

  /** Location der Antwort: eigener Server wird zum Ingress-Pfad, Relatives bekommt ihn davor, Fremdes entfällt */
  function rewriteLocation(loc, base) {
    try {
      const u = new URL(loc, target);
      if (u.origin !== target.origin) return undefined;
      const out = base + u.pathname + u.search + u.hash;
      return out.replace(/^\/{2,}/, "/"); // ohne Ingress-Pfad würde "//host" ein Ziel auf einem fremden Host
    } catch { return undefined; }
  }

  function apiProxy(req, res, p) {
    const base = ingressBase(req);
    const fail = () => { if (!res.headersSent) res.writeHead(502, { "Content-Type": "text/plain" }); res.end("HeyClima-Server nicht erreichbar"); };
    const out = lib.request({ ...connOptions, method: req.method, path: p, headers: upstreamHeaders(req, target) }, (up) => {
      const headers = {};
      for (const [k, v] of Object.entries(up.headers)) if (!DROP_RESPONSE.test(k)) headers[k] = v;
      if (headers["set-cookie"]) headers["set-cookie"] = headers["set-cookie"].map((c) => rewriteCookie(c, base));
      if (headers.location !== undefined) {
        const loc = rewriteLocation(String(headers.location), base);
        if (loc === undefined) delete headers.location; else headers.location = loc;
      }
      res.writeHead(up.statusCode ?? 502, headers);
      up.pipe(res);
      up.on("error", () => res.destroy());
    });
    out.setTimeout(upstreamTimeoutMs, () => out.destroy(new Error("Zeitüberschreitung")));
    out.on("error", fail);
    res.on("close", () => out.destroy());
    req.pipe(out);
  }

  function serveIndex(req, res) {
    fs.readFile(path.join(root, "index.html"), "utf8", (err, html) => {
      if (err) { res.writeHead(404, { "Content-Type": "text/plain" }); return res.end("Web-App fehlt"); }
      const base = ingressBase(req);
      const out = html
        .replace(/(\s(?:src|href)=["'])\/(?!\/)/g, `$1${base}/`)
        .replace(/<head[^>]*>/i, (m) => `${m}<script>window.__HEYCLIMA_BASE__=${js(base)};window.__HEYCLIMA_SERVER__=${js(target.origin)};window.__HEYCLIMA_MODE__=${js(mode)};window.__HEYCLIMA_CONTROL__=${js(haControl)};</script>`);
      res.writeHead(200, { "Content-Type": TYPES[".html"], "Cache-Control": "no-store" });
      res.end(req.method === "HEAD" ? undefined : out);
    });
  }

  function serveStatic(req, res, pathname) {
    let rel;
    try { rel = decodeURIComponent(pathname); } catch { res.writeHead(400); return res.end(); }
    if (rel.includes("\0") || rel.split(/[\\/]/).includes("..")) { res.writeHead(400); return res.end(); }
    if (rel === "/" || rel === "/index.html") return serveIndex(req, res);
    const file = path.resolve(root, "." + rel);
    if (file !== root && !file.startsWith(root + path.sep)) { res.writeHead(400); return res.end(); }
    fs.stat(file, (err, st) => {
      if (err || !st.isFile()) {
        if (path.extname(rel)) { res.writeHead(404, { "Content-Type": "text/plain" }); return res.end("nicht gefunden"); }
        return serveIndex(req, res); // Pfade der App ohne Datei: index.html
      }
      res.writeHead(200, { "Content-Type": TYPES[path.extname(file).toLowerCase()] ?? "application/octet-stream", "Content-Length": st.size });
      if (req.method === "HEAD") return res.end();
      const rs = fs.createReadStream(file);
      rs.on("error", () => res.destroy());
      rs.pipe(res);
    });
  }

  const server = http.createServer((req, res) => {
    if (!isAllowed(req.socket.remoteAddress, allowedIps)) { res.writeHead(403, { "Content-Type": "text/plain" }); return res.end("nur über Home Assistant"); }
    const p = safeTarget(req.url);
    if (!p) { res.writeHead(400, { "Content-Type": "text/plain" }); return res.end(); }
    const pathname = p.split("?")[0];
    if (pathname === "/api" || pathname.startsWith("/api/")) {
      if (haMode) { res.writeHead(404, { "Content-Type": "text/plain" }); return res.end("im Modus homeassistant gibt es kein /api"); }
      return apiProxy(req, res, p);
    }
    if (haMode && pathname === "/ha/ws") { res.writeHead(426, { "Content-Type": "text/plain", Upgrade: "websocket" }); return res.end("nur WebSocket"); }
    if (req.method !== "GET" && req.method !== "HEAD") { res.writeHead(405, { Allow: "GET, HEAD" }); return res.end(); }
    serveStatic(req, res, pathname);
  });

  // Verbindungen nach dem WebSocket-Upgrade kennt der http-Server nicht mehr (closeAllConnections erreicht sie nicht)
  const upgraded = new Set();
  const closeAll = server.closeAllConnections.bind(server);
  server.closeAllConnections = () => { closeAll(); for (const s of upgraded) s.destroy(); };
  const track = (...socks) => { for (const s of socks) { upgraded.add(s); s.on("close", () => upgraded.delete(s)); } };

  server.on("upgrade", (req, socket, clientHead) => {
    const p = safeTarget(req.url);
    if (haMode && p && p.split("?")[0] === "/ha/ws") {
      if (!isAllowed(req.socket.remoteAddress, allowedIps)) return socket.destroy();
      if (haSockets.size >= maxHaSockets) return void socket.end("HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n");
      if (!acceptHandshake(req, socket)) return;
      track(socket);
      relay(socket, clientHead, { token: supervisorToken, upstreamUrl: haWsUrl, control: haControl, rateLimits: limits, totalWindow, modeGapMs, modeAt, ...(writeTimeoutMs ? { writeTimeoutMs } : {}), ...(WebSocketImpl ? { WebSocketImpl } : {}), ...(authTimeoutMs ? { authTimeoutMs } : {}), ...(pingMs ? { pingMs } : {}) });
      haSockets.add(socket);
      socket.on("close", () => haSockets.delete(socket));
      return;
    }
    if (haMode || !isAllowed(req.socket.remoteAddress, allowedIps) || !p || p.split("?")[0] !== "/api/events") return socket.destroy();
    const out = lib.request({ ...connOptions, method: "GET", path: p, headers: { ...upstreamHeaders(req, target), connection: "Upgrade", upgrade: "websocket" } });
    out.setTimeout(upstreamTimeoutMs, () => out.destroy(new Error("Zeitüberschreitung"))); // bis zum Upgrade; danach kein Zeitlimit
    out.on("upgrade", (up, upSocket, head) => {
      out.setTimeout(0); upSocket.setTimeout(0);
      track(socket, upSocket);
      let raw = `HTTP/1.1 ${up.statusCode} ${up.statusMessage}\r\n`;
      for (let i = 0; i < up.rawHeaders.length; i += 2) raw += `${up.rawHeaders[i]}: ${up.rawHeaders[i + 1]}\r\n`;
      socket.write(raw + "\r\n");
      if (head?.length) socket.write(head);
      if (clientHead?.length) upSocket.write(clientHead);
      upSocket.pipe(socket); socket.pipe(upSocket);
      upSocket.on("error", () => socket.destroy()); socket.on("error", () => upSocket.destroy());
      upSocket.on("close", () => socket.destroy()); socket.on("close", () => upSocket.destroy());
    });
    out.on("response", (up) => { // Upstream lehnt ab (z. B. 401): Antwort durchreichen und schließen
      socket.end(`HTTP/1.1 ${up.statusCode} ${up.statusMessage}\r\nConnection: close\r\n\r\n`);
      up.resume();
    });
    out.on("error", () => socket.destroy());
    socket.on("error", () => out.destroy());
    socket.on("close", () => out.destroy());
    out.end();
  });

  return server;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const serverUrl = process.env.SERVER_URL || "https://app.heyclima.com";
  const webDir = process.env.WEB_DIR || path.join(path.dirname(fileURLToPath(import.meta.url)), "web");
  const allowed = (process.env.ALLOWED_IPS || SUPERVISOR).split(",").map((s) => s.trim()).filter(Boolean);
  const port = Number(process.env.PORT || 8099);
  const mode = process.env.MODE || "heyclima";
  const supervisorToken = process.env.SUPERVISOR_TOKEN || "";
  const control = process.env.CONTROL === "true"; // nur genau "true"; sonst nur Anzeige
  // Nur für Kindprozesse (process.env wird vererbt); /proc/self/environ dieses Prozesses enthält den Token weiter. Schutz davor
  // gibt es nicht: der Proxy ist der einzige Prozess im Container und braucht ihn.
  delete process.env.SUPERVISOR_TOKEN;
  const server = createProxy({ serverUrl, webDir, allowedIps: allowed, mode, supervisorToken, control, haWsUrl: process.env.HA_WS_URL || HA_WS_URL });
  if (mode === "homeassistant" && !supervisorToken) console.warn("MODE=homeassistant, aber kein SUPERVISOR_TOKEN (homeassistant_api in config.yaml?)");
  server.listen(port, "0.0.0.0", () => console.log(`HeyClima-Proxy auf Port ${server.address().port}, ${mode === "homeassistant" ? `Modus homeassistant (${control ? "Steuerung ein" : "nur Anzeige"})` : `Server ${serverUrl}`}`));
  // sauber beenden, wenn Home Assistant die App stoppt (docker stop sendet SIGTERM)
  for (const sig of ["SIGTERM", "SIGINT"]) process.on(sig, () => { server.close(() => process.exit(0)); server.closeAllConnections(); });
}
