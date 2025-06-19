# -------- Giai đoạn 1: Build ứng dụng bằng Maven với JDK 21 --------
FROM maven:3.9.6-eclipse-temurin-21 AS builder

WORKDIR /app

# Copy POM và resolve dependencies trước để tận dụng cache
COPY pom.xml .
RUN mvn dependency:go-offline

# Copy toàn bộ mã nguồn
COPY src ./src

# Build ứng dụng (không chạy test)
RUN mvn clean package -DskipTests

# -------- Giai đoạn 2: Image nhẹ chỉ để chạy JAR --------
FROM eclipse-temurin:21-jdk-jammy

WORKDIR /app

# Copy file JAR đã build từ stage trước
COPY --from=builder /app/target/insertdata-0.0.1-SNAPSHOT.jar app.jar

# Chạy ứng dụng Spring Boot
ENTRYPOINT ["java", "-jar", "app.jar"]
