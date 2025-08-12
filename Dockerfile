FROM semtech/mu-python-template:2.0.0-beta.2
LABEL org.opencontainers.image.authors="info@redpencil.io"

ENV SCRAPY_SETTINGS_MODULE="lblod.settings"
ENV AUTO_RUN="false"
ENV DEFAULT_GRAPH="http://mu.semte.ch/graphs/scraper-graph"
ENV PYTHONUNBUFFERED="1"
ENV IN_DOCKER="true"
COPY scrapy.cfg /usr/src/app