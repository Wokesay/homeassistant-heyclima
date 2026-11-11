# HeyClima-App für Home Assistant: kleiner Node-Proxy, der die Web-App ausliefert und /api an den HeyClima-Server leitet.
# Der Web-Export der App (Ordner web/) kommt beim Bauen des Images hinein (Bauschritte: nur im privaten Entwicklungs-Repo beschrieben).
ARG BUILD_FROM=node:22-alpine
FROM ${BUILD_FROM}

COPY rootfs/ /
COPY web/ /app/web/
RUN chmod +x /run.sh

EXPOSE 8099
CMD ["/run.sh"]
