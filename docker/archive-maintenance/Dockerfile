FROM maven:3.9.8-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml /src/pom.xml
COPY src /src/src
RUN mvn -q -DskipTests package

FROM bitnami/spark:3.5.1
USER root
COPY --from=build /src/target/archive-maintenance.jar /opt/app/archive-maintenance.jar
USER 1001
