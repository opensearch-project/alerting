ARG DOCKER_VERSION_ARG
FROM opensearchstaging/opensearch:${DOCKER_VERSION_ARG}
#FROM phaseshiftstudio/opensearch:2.4.0-old-with-security
SHELL ["/bin/bash","-x","-e","-c"]
ARG PLUGIN_ARG
ENV PLUGIN ${PLUGIN_ARG}
RUN if [ -d /usr/share/opensearch/plugins/opensearch-alerting ]; then /usr/share/opensearch/bin/opensearch-plugin remove opensearch-alerting; fi;

ADD build/distributions/$PLUGIN /tmp/
ADD ./ /opt/opensearch-alerting
USER root
#COPY --chown=opensearch:opensearch ci/roles.yml /usr/share/opensearch/config/opensearch-security/roles.yml
RUN chown -R opensearch:opensearch /opt/opensearch-alerting
USER opensearch
RUN /usr/share/opensearch/bin/opensearch-plugin install --batch file:/tmp/$PLUGIN