# syntax=docker/dockerfile:1

FROM maven:3.9.1-amazoncorretto-19-debian AS build
RUN apt-get update && apt-get install -y protobuf-compiler
COPY src /usr/src/logging-service/src
COPY pom.xml /usr/src/logging-service
RUN mvn -f /usr/src/logging-service/pom.xml clean package

FROM gcr.io/distroless/java
COPY --from=build /usr/src/logging-service/target/logging-service-0.1-shaded.jar /usr/logging-service/logging-service-0.1-shaded.jar
ENTRYPOINT ["java","-jar","/usr/logging-service/logging-service-0.1-shaded.jar"]
