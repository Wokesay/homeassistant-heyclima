# Änderungen

## 0.2.0

- Option `control` (Vorgabe `false`): mit `true` kann die Oberfläche im Modus `homeassistant` die `climate.*`-Entitäten steuern (Betriebsart, Soll, Lüfter, Schwenken, Aus/Ein). Der Relay lässt dafür nur `call_service` mit Domäne `climate` und den Diensten `set_hvac_mode`, `set_temperature`, `set_fan_mode`, `set_swing_mode` für eine einzelne, dem Browser gemeldete Entität durch, baut die Anfrage neu auf, prüft Werte gegen Listen und Bereich der Entität, begrenzt die Rate (5/s je Verbindung, 60/min insgesamt) und meldet nur ok oder einen festen Fehlercode. Ohne `control` bleibt alles wie in der reinen Anzeige (nur lesen). Bei einem Fehler zeigt die Oberfläche wieder den Zustand von Home Assistant mit Hinweis. Ungeprüft gegen ein echtes Home Assistant; vor dem Einschalten streng prüfen (DOCS.md, „Steuern“).
- Nachbesserung nach der strengen Prüfung: je Gerät nur eine Sendung zugleich (Reihenfolge Betriebsart, Soll), „Aus“ und Betriebsarten nur, wenn die Entität sie anbietet, Hinweis statt stillem Verwerfen, Regler gesperrt bei Entitäten ohne einzelnen Sollwert, Bestätigung gegen die tatsächlich gesendeten Werte, Text bei abgebrochener Verbindung („Ergebnis unklar“); Relay: `busy` vor dem Kontingent, Betriebsart je Entität höchstens alle 3 s.

- Stufe 2, nur Anzeige: Option `mode: heyclima | homeassistant`. Im Modus `homeassistant` zeigt die App die `climate.*`-Entitäten von Home Assistant, ohne HeyClima-Konto. Der Proxy bekommt den Supervisor-Token (`homeassistant_api: true`) und stellt `/ha/ws` bereit: ein Relay, der nur `auth`, `ping`, `get_states` und `subscribe_events` (`state_changed`) durchlässt und auf `climate` filtert; der Token erreicht den Browser nie. Kein Schreiben.

- Web-App läuft unter dem Ingress-Pfad (Serveradresse, WebSocket, Verlauf, Hilfe-Links, gemerkte Anmeldung bei wechselndem Token); Browser-Test hinter dem Proxy.
- Proxy: SIGTERM beendet auch bei offenem WebSocket, Zeitlimit bis zum WebSocket-Upgrade, `Location` wird bei leerem Ingress-Pfad nie zu `//host`, `window.__HEYCLIMA_SERVER__` für Links auf den echten Server.

## 0.1.0 (Gerüst)

- Erstes Gerüst: Ingress-Eintrag „HeyClima“, Proxy (Web-App ausliefern, `/api` und WebSocket `/api/events` an `server_url` leiten, nur Supervisor-Ingress).
