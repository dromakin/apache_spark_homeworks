FROM confluentinc/cp-server-connect-operator:6.1.0.0 AS base

USER root

RUN mkdir -p /opt/confluent-hub-client \
    && curl "http://client.hub.confluent.io/confluent-hub-client-latest.tar.gz" \
    | tar -xzv -C /opt/confluent-hub-client
ENV PATH="/opt/confluent-hub-client/bin:${PATH}"

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-azure-blob-storage:1.6.2 \
    && confluent-hub install --no-prompt confluentinc/kafka-connect-azure-blob-storage-source:1.4.5

USER user
