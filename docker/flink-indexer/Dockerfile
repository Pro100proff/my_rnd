FROM maven:3.9.8-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml /src/pom.xml
COPY src /src/src
RUN mvn -q -DskipTests package

FROM flink:1.19.2-scala_2.12-java17
USER root
COPY --from=build /src/target/archive-maintenance.jar /opt/app/archive-maintenance.jar
COPY docker/flink-indexer/submit.sh /opt/app/submit.sh
RUN chmod +x /opt/app/submit.sh
USER flink
ENTRYPOINT ["/opt/app/submit.sh"]
