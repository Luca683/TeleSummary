FROM fluent/fluentd:v1.12.0-debian-1.0

COPY ./conf/fluentd.conf /fluentd/etc/fluentd.conf

USER root

RUN fluent-gem install fluent-plugin-kafka

EXPOSE 9700

CMD ["fluentd", "-c", "/fluentd/etc/fluentd.conf"]
