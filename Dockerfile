# ── Stage 1: Build ───────────────────────────────────────────────────────────
# Use a full JDK image to compile and package the application.
FROM eclipse-temurin:21-jdk-alpine AS build

WORKDIR /workspace

# Copy Maven wrapper and POM first (layer caching for dependency downloads)
COPY mvnw .
COPY .mvn .mvn
COPY pom.xml .
RUN chmod +x mvnw

# Download dependencies (cached unless pom.xml changes)
RUN ./mvnw dependency:go-offline -B

# Copy source and build the application
COPY src src
RUN ./mvnw package -DskipTests -B

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
# Use a slim JRE-only image to minimize the attack surface.
FROM eclipse-temurin:21-jre-alpine AS runtime

# Create a non-root user for security
RUN addgroup -S aegis && adduser -S aegis -G aegis

WORKDIR /app

# Copy only the fat JAR from the build stage
COPY --from=build /workspace/target/*.jar app.jar

# Switch to non-root user
USER aegis

# Expose the application port
EXPOSE 8080

# Health check for container orchestrators (Render, Railway, Docker Compose)
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD wget -q --spider http://localhost:8080/actuator/health || exit 1

# JVM tuning for containerized environments:
#  - UseContainerSupport: respect cgroup memory limits
#  - MaxRAMPercentage: use up to 75% of available container RAM
#  - ExitOnOutOfMemoryError: fail fast rather than degrade silently
ENTRYPOINT ["java", \
  "-XX:+UseContainerSupport", \
  "-XX:MaxRAMPercentage=75.0", \
  "-XX:+ExitOnOutOfMemoryError", \
  "-Djava.security.egd=file:/dev/./urandom", \
  "-jar", "app.jar"]
