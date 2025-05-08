plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"
val flinkVersion = "1.18.1"
val slf4jVersion = "1.7.36"
val log4jVersion = "2.17.1"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    implementation("org.apache.flink:flink-java:${flinkVersion}")
    implementation("org.apache.flink:flink-streaming-java:${flinkVersion}")
    implementation("org.apache.flink:flink-clients:${flinkVersion}")
    // Logging with Log4j 2
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:$log4jVersion")
    runtimeOnly("org.apache.logging.log4j:log4j-core:$log4jVersion")
    runtimeOnly("org.apache.logging.log4j:log4j-api:$log4jVersion")
}

tasks.test {
    useJUnitPlatform()
}