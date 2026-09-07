# =============================================================================
# edge-service
#
# The build context is apps/, because this service depends on the in-repo
# mqtt-spring-boot modules and Maven needs them available. The sibling
# Dockerfile.dockerignore excludes the other service, so it never reaches the
# daemon — the isolation of a per-service context, without giving up the local
# starter build. Publish the starter to a registry and the context narrows back
# to this directory on its own.
#
# Stage 1 resolves dependencies in their own layer, so a source-only change does
# not re-download the world. Stage 2 explodes the Spring Boot layered jar so
# dependencies (rarely change) and application classes (change every commit)
# land in separate image layers — a code push then ships a few hundred kB
# instead of 60MB, which matters when you are shipping to gateways over the same
# links this demo is about.
# =============================================================================

# --- stage 1: build ----------------------------------------------------------
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

# 1. Build and install the starter (autoconfigure + starter) into the local repo.
COPY mqtt-spring-boot/pom.xml mqtt-spring-boot/
COPY mqtt-spring-boot/mqtt-spring-boot-autoconfigure/pom.xml mqtt-spring-boot/mqtt-spring-boot-autoconfigure/
COPY mqtt-spring-boot/mqtt-spring-boot-starter/pom.xml mqtt-spring-boot/mqtt-spring-boot-starter/
RUN mvn -B -q -f mqtt-spring-boot/pom.xml dependency:go-offline -DskipTests
COPY mqtt-spring-boot/mqtt-spring-boot-autoconfigure/src mqtt-spring-boot/mqtt-spring-boot-autoconfigure/src
RUN mvn -B -q -f mqtt-spring-boot/pom.xml install -DskipTests

# 2. Build the service against it. Dependencies resolve in their own layer, so a
#    source-only change does not re-download anything.
COPY edge-service/pom.xml edge-service/
RUN mvn -B -q -f edge-service/pom.xml dependency:go-offline -DskipTests
COPY edge-service/src edge-service/src
RUN mvn -B -q -f edge-service/pom.xml clean package -DskipTests

# The layered jar splits into four directories: dependencies, spring-boot-loader,
# snapshot-dependencies and application — copied below least- to most-frequently
# changed.
RUN java -Djarmode=tools -jar edge-service/target/edge-service.jar extract --layers --destination /extracted \
 && mv /extracted/application/edge-service.jar /extracted/application/app.jar

# --- stage 2: runtime --------------------------------------------------------
FROM eclipse-temurin:21-jre-jammy AS runtime

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system --gid 10001 app \
 && useradd --system --uid 10001 --gid app --home /app app \
 && mkdir -p /app /var/lib/mqtt-client \
 && chown -R app:app /app /var/lib/mqtt-client

WORKDIR /app
COPY --from=build --chown=app:app /extracted/dependencies/ ./
COPY --from=build --chown=app:app /extracted/spring-boot-loader/ ./
COPY --from=build --chown=app:app /extracted/snapshot-dependencies/ ./
COPY --from=build --chown=app:app /extracted/application/ ./

USER app:app
EXPOSE 8080

# Paho's file persistence store lives here. It must be a volume: it holds
# in-flight QoS 1 messages, and losing it on restart loses those messages.
VOLUME ["/var/lib/mqtt-client"]

ENV JAVA_TOOL_OPTIONS="\
-XX:MaxRAMPercentage=75.0 \
-XX:+ExitOnOutOfMemoryError \
-Duser.timezone=UTC"

ENTRYPOINT ["java", "-jar", "app.jar"]
