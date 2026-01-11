# Stage 1
FROM maven:3.9.4-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

# Stage 2
FROM eclipse-temurin:17-jdk-jammy
WORKDIR /app


ENV JAVA_TOOL_OPTIONS=""

COPY --from=build /app/target/*.jar app.jar


ENTRYPOINT ["java", "--add-opens", "java.base/sun.util.calendar=ALL-UNNAMED", "-jar", "app.jar"]
