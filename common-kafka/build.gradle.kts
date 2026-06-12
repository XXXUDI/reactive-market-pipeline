plugins {
    `java-library`
}

group = "com.soCompany"
version = "0.0.1-SNAPSHOT"
description = "common-kafka"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-kafka:4.0.6")
    implementation("org.apache.kafka:kafka-streams:3.9.0")
}
