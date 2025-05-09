plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"
val flinkVersion = "1.17.1"
val log4jVersion = "2.17.1"
val lombokVersion = "1.18.30"
val jacksonVersion = "2.15.2"

repositories {
    mavenCentral()
    maven {
        url = uri("https://repository.apache.org/content/repositories/releases/")
    }
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    implementation("org.apache.flink:flink-java:${flinkVersion}")
    implementation("org.apache.flink:flink-streaming-java:${flinkVersion}")
    implementation("org.apache.flink:flink-clients:${flinkVersion}")
    // Correct connector
    implementation("org.apache.flink:flink-connector-kafka:${flinkVersion}")
    //  Log4j 2 logging (for org.apache.logging.log4j.Logger)
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:${log4jVersion}")
    implementation("org.apache.logging.log4j:log4j-core:${log4jVersion}")
    implementation("org.apache.logging.log4j:log4j-api:${log4jVersion}")
    // --- JSON support (Jackson) ---
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    implementation("com.fasterxml.jackson.core:jackson-core:$jacksonVersion")
    implementation("com.fasterxml.jackson.core:jackson-annotations:$jacksonVersion")

    // --- Lombok (auto boilerplate) ---
    compileOnly("org.projectlombok:lombok:$lombokVersion")
    annotationProcessor("org.projectlombok:lombok:$lombokVersion")

}

tasks.test {
    useJUnitPlatform()
}