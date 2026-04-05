# ─────────────────────────────────────────────────────────────────────────────
# Multi-stage Dockerfile for payment-workflow-service
#
# Stage 1 (builder): Compiles the JAR using Maven
# Stage 2 (runtime): Minimal JRE image — no build tools, smaller attack surface
#
# OpenShift note: OpenShift runs containers as a random non-root UID by default.
# We set file permissions so any UID in group 0 can write to the work directory.
# This is the Red Hat recommended pattern for OpenShift-compatible images.
# ─────────────────────────────────────────────────────────────────────────────

# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM maven:3.9.6-eclipse-temurin-17 AS builder

WORKDIR /build

# Copy pom first — Docker caches this layer until pom changes
COPY pom.xml .
RUN mvn dependency:go-offline -q

# Copy source and build
COPY src ./src
RUN mvn clean package -DskipTests -q

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

# Create a non-root user (UID 1001) but also allow OpenShift's arbitrary UID
# to run by making the group (0) own the directory
RUN addgroup -S appgroup && adduser -S appuser -G appgroup -u 1001 \
    && chown -R 1001:0 /app \
    && chmod -R g=u /app

# Copy the fat JAR from builder stage
COPY --from=builder --chown=1001:0 /build/target/payment-workflow-1.0.0.jar app.jar

# Health check — used by OpenShift liveness/readiness probes
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD wget -qO- http://localhost:8080/actuator/health || exit 1

USER 1001

EXPOSE 8080

# JVM tuning for containerised environments:
#   -XX:+UseContainerSupport        — respects cgroup CPU/memory limits
#   -XX:MaxRAMPercentage=75.0       — use max 75% of container memory for heap
#   -XX:+ExitOnOutOfMemoryError     — crash fast instead of thrashing
ENTRYPOINT ["java", \
    "-XX:+UseContainerSupport", \
    "-XX:MaxRAMPercentage=75.0", \
    "-XX:+ExitOnOutOfMemoryError", \
    "-Djava.security.egd=file:/dev/./urandom", \
    "-jar", "app.jar"]
