FROM eclipse-temurin:21-jdk-jammy AS build
ARG DEBIAN_FRONTEND=noninteractive
# RUN apt-get update && apt-get install curl coreutils -y
ENV HOME=/usr/app
RUN mkdir -p $HOME
WORKDIR $HOME
ADD . $HOME
RUN chmod +x ./mvnw
RUN ./mvnw -am clean install -DskipTests
RUN cd -

FROM gcr.io/distroless/java21-debian12
ARG JAR_FILE=/usr/app/target/*.jar
COPY --from=build $JAR_FILE /app/runner.jar
ENV SERVER_PORT=8080
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/runner.jar"]
