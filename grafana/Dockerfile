FROM openjdk:11-jre-slim
LABEL maintainer="j.feinauer@pragmaticminds.de"
RUN mkdir /iotdb

ADD target/iotdb-grafana-*.war /iotdb/iotdb-grafana.war
ADD src/main/resources/application.properties /iotdb/

EXPOSE 8888
EXPOSE 4000

WORKDIR /iotdb/
ENTRYPOINT ["java", "-Dspring.datasource.url=jdbc:iotdb://${IOTDB_HOST}:${IOTDB_PORT}", "-Dspring.datasource.username=${IOTDB_USER}", "-Dspring.datasource.password=${IOTDB_PASSWORD}", "-jar", "/iotdb/iotdb-grafana.war"]
