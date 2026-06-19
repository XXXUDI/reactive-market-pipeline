plugins {
    java
    id("org.springframework.boot") version "4.0.6"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "com.soCompany"
version = "0.0.1-SNAPSHOT"
description = "data-processor"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

repositories {
    mavenCentral()
}

dependencies {

    // Internal modules
    implementation(project(":common-events"))
    implementation(project(":common-kafka"))

    // Apache Flink
    implementation("org.apache.flink:flink-streaming-java:1.20.0")
    implementation("org.apache.flink:flink-clients:1.20.0")

    // Flink Kafka Connector
    implementation("org.apache.flink:flink-connector-kafka:3.3.0-1.20")
    implementation("org.apache.flink:flink-connector-base:1.20.0")

    // Jackson for serialization / deserialization
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.2")

    // Logging
    implementation("org.slf4j:slf4j-api:2.0.9")

    implementation("org.springframework.boot:spring-boot-starter-kafka")
    implementation("org.apache.kafka:kafka-streams")
    compileOnly("org.projectlombok:lombok")
    annotationProcessor("org.projectlombok:lombok")
    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")

    // = = = = = = Test dependencies = = = = = = =
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testCompileOnly("org.projectlombok:lombok")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testAnnotationProcessor("org.projectlombok:lombok")
}

tasks.withType<Test> {
    useJUnitPlatform()
}
