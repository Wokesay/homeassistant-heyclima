# HeyClima für Home Assistant

## Was ist das?

Diese App zeigt die HeyClima-Oberfläche als Eintrag „HeyClima“ in der Seitenleiste von Home Assistant: Thermostat-Karte für deine Klimageräte, bedienbar wie in der HeyClima-App. Es gibt zwei Betriebsarten (Option `mode`, siehe unten).

**Vorabversion:** Die App wurde bisher nicht an echten Geräten in einer echten Home-Assistant-Installation geprüft, sondern nur gegen eine Nachbildung von Home Assistant. Es kann Unterschiede geben, die erst im Alltag auffallen. Probiere sie zuerst mit einem unkritischen Gerät aus.

Das ist ein Zusatz zu HeyClima und kein offizielles Home-Assistant-Produkt („läuft in Home Assistant“, nicht „Works with Home Assistant“).

## Installieren

1. Home Assistant: Einstellungen > Apps (früher Add-ons) > App-Shop > Menü (drei Punkte) > Repositories.
2. Die Adresse dieses Repositories eintragen (steht oben auf der Seite, von der du diese Anleitung hast) und hinzufügen.
3. „HeyClima“ installieren und starten, „In Seitenleiste anzeigen“ einschalten.

## Optionen

| Option | Vorgabe | Bedeutung |
|---|---|---|
| `server_url` | `https://app.heyclima.com` | Adresse des HeyClima-Servers. Nur ändern, wenn du einen eigenen Server betreibst. Mit `http://` gehen Anmelde-Token und Daten unverschlüsselt über das Netz; das nur im eigenen Heimnetz zum Testen verwenden. Gilt nur im Modus `heyclima`. |
| `mode` | `heyclima` | `heyclima`: Die App zeigt deine HeyClima-Geräte über den HeyClima-Server; du meldest dich mit deinem HeyClima-Konto an. `homeassistant`: Die App zeigt die Klimageräte (`climate.*`) von Home Assistant selbst, ohne HeyClima-Konto und ohne HeyClima-Server. Dort kann sie die Geräte nur anzeigen; steuern kann sie nur, wenn zusätzlich `control` eingeschaltet ist. |
| `control` | `false` | Nur im Modus `homeassistant`: erlaubt der Oberfläche, die Klimageräte über Home Assistant zu steuern (Betriebsart, Solltemperatur, Lüfter, Schwenken, Aus und Ein). Im Modus `heyclima` hat die Option keine Wirkung. |

**Warnung zu `control`:** Standard ist aus. Wer es einschaltet, erlaubt allen Nutzern mit Zugriff auf das Panel „HeyClima“ das Steuern der Klimageräte in Home Assistant, auch Nutzern ohne Administratorrechte. Home Assistant kennt hier keine Rechte je Nutzer und Gerät. In der Aktivitätsliste erscheint die Änderung vermutlich nicht unter dem Namen der Person. Schalte es nur ein, wenn du allen vertraust, die das Panel sehen.

## Die beiden Betriebsarten

**`heyclima` (Vorgabe).** Die App ist die HeyClima-Oberfläche mit allen Funktionen deines Kontos: Zeitpläne, Gäste, Verläufe, Fühler, Freigaben. Die Geräte werden wie in der HeyClima-App über den HeyClima-Server gesteuert.

**`homeassistant`.** Die App zeigt die `climate`-Geräte, die Home Assistant kennt, auch von anderen Herstellern, ohne Konto. Gesteuert wird ausschließlich über Home Assistant (nur mit `control`). Dafür bekommt die App Zugriff auf die Schnittstelle von Home Assistant; sie reicht davon nur das Lesen und, mit `control`, das Setzen von Betriebsart, Soll, Lüfter und Schwenken an `climate`-Geräten weiter.

## Grenzen

- Im Modus `homeassistant` gibt es ohne HeyClima-Konto keine Zeitpläne, Schlaf-Timer, Szenen, Gäste, Verläufe, Fühler, Freigaben und keine Einstellungen für Konto oder Bridge. Das bleibt der HeyClima-App und dem Modus `heyclima` vorbehalten.
- Temperaturen werden nur in °C angezeigt und gesetzt. Läuft Home Assistant in Fahrenheit, stimmen die Zahlen nicht.
- Geräte ohne einzelnen Sollwert (nur Bereich) lassen sich nur in der Betriebsart bedienen; „Aus“ und Betriebsarten erscheinen nur, wenn das Gerät sie anbietet.
- Gemeldete Änderungen kommen so schnell an, wie die Integration in Home Assistant sie meldet. Meldet sie eine Änderung nicht, springt die Anzeige nach etwa 30 Sekunden auf den Zustand von Home Assistant zurück.

## Datenschutz und Sitzungstoken

- Home Assistant zeigt die App per Ingress: kein offener Port, Zugriff nur für angemeldete Home-Assistant-Nutzer.
- **Modus `heyclima`:** Dein Anmelde-Token (Sitzungstoken) liegt im Speicher des Browsers, und zwar für den Ursprung (Adresse) deiner Home-Assistant-Installation. **Andere Apps und Karten, die in Home Assistant auf derselben Adresse laufen, können ihn lesen.** Installiere in einer solchen Installation nur Apps und Karten, denen du vertraust, oder nutze den Modus `homeassistant`, der kein HeyClima-Konto braucht.
- Im Modus `homeassistant` gehen keine Daten an den HeyClima-Server.
- Hilfe, Datenschutz und Impressum öffnen die Seiten von heyclima.com im Browser.

## Änderungen

Siehe [CHANGELOG.md](CHANGELOG.md).
