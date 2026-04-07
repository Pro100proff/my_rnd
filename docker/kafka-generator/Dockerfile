FROM maven:3.9.8-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml /src/pom.xml
COPY src /src/src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /opt/app
COPY --from=build /src/target/archive-maintenance.jar /opt/app/archive-maintenance.jar
ENTRYPOINT ["java", "-cp", "/opt/app/archive-maintenance.jar", "com.platform.archive.tools.RawEventGenerator"]
