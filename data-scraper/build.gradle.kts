plugins {
    java
    id("org.springframework.boot") version "4.0.6"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "com.soCompany"
version = "0.0.1-SNAPSHOT"
description = "data-scraper"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    // Include common-events
    implementation(project(":common-events"))
    implementation(project(":common-kafka"))
    // Reactive Kafka
    implementation("org.springframework.kafka:spring-kafka")
    implementation("io.projectreactor.kafka:reactor-kafka:1.3.25")
    // WebClient
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    // Lombok
    compileOnly("org.projectlombok:lombok")
    annotationProcessor("org.projectlombok:lombok")
    // = = = = = = Test dependencies = = = = = =
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testCompileOnly("org.projectlombok:lombok")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testAnnotationProcessor("org.projectlombok:lombok")

    testImplementation("io.projectreactor:reactor-test")
}

tasks.withType<Test> {
    useJUnitPlatform()
}
