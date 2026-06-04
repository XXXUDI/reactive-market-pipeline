plugins {
    `java-library`
}

group = "com.soCompany"
version = "0.0.1-SNAPSHOT"
description = "common-events"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.18.2")
}
