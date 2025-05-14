// build.gradle.kts
plugins {
    java
    application
    id("com.github.johnrengelman.shadow") version "7.1.2"
    id("io.freefair.lombok") version "6.6.2"
}

// Project configuration
group = "com.ecommerce.analytics"
version = "1.0.0"
description = "E-commerce Analytics Platform - Flink Processing"

// Configure application plugin
application {
    mainClass.set("com.ecommerce.analytics.RealTimeAnalytics")
}

// Versions
val flinkVersion = "1.17.1"
val log4jVersion = "2.17.1"
val lombokVersion = "1.18.30"
val jacksonVersion = "2.15.2"
val mongodbVersion = "4.7.1"

repositories {
    mavenCentral()
    maven {
        url = uri("https://repository.apache.org/content/repositories/releases/")
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

dependencies {
    // Flink core dependencies
    implementation("org.apache.flink:flink-java:${flinkVersion}")
    implementation("org.apache.flink:flink-streaming-java:${flinkVersion}")
    implementation("org.apache.flink:flink-clients:${flinkVersion}")
    implementation("org.apache.flink:flink-runtime-web:${flinkVersion}")

    // Kafka connector
    implementation("org.apache.flink:flink-connector-kafka:${flinkVersion}")

    // MongoDB connector for storing events (optional)
//    implementation("org.mongodb:mongodb-driver-sync:${mongodbVersion}")
    implementation("org.mongodb:mongodb-driver-sync:4.7.1")
    // JSON processing with Jackson
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    implementation("com.fasterxml.jackson.core:jackson-core:$jacksonVersion")
    implementation("com.fasterxml.jackson.core:jackson-annotations:$jacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")

    // Logging
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:${log4jVersion}")
    implementation("org.apache.logging.log4j:log4j-core:${log4jVersion}")
    implementation("org.apache.logging.log4j:log4j-api:${log4jVersion}")

    // Lombok (auto boilerplate)
    compileOnly("org.projectlombok:lombok:$lombokVersion")
    annotationProcessor("org.projectlombok:lombok:$lombokVersion")

    // Testing
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.mockito:mockito-core:4.11.0")
    testImplementation("org.hamcrest:hamcrest-all:1.3")
}

// Configure Shadow plugin for creating fat JARs
tasks.shadowJar {
    archiveBaseName.set("ecommerce-analytics-flink")
    archiveClassifier.set("all")
    // Add service files from all dependencies (important for some connectors)
    mergeServiceFiles()
    // Exclude signature files that can cause conflicts
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")

    // Log the size of the resulting JAR when built
    doLast {
        logger.quiet("Shadow JAR created: ${archiveFile.get().asFile.path}")
        logger.quiet("Size: ${archiveFile.get().asFile.length() / (1024 * 1024)} MB")
    }
}

// Configure the test task
tasks.test {
    useJUnitPlatform()
    // Enable logging during tests
    testLogging {
        events("passed", "skipped", "failed")
    }
}

// Make the standard jar task dependant on shadowJar
tasks.jar {
    dependsOn(tasks.shadowJar)
    // Add main class to manifest
    manifest {
        attributes(
            "Main-Class" to "com.ecommerce.analytics.RealTimeAnalytics",
            "Implementation-Title" to project.name,
            "Implementation-Version" to project.version
        )
    }
}

// Custom task for easier execution
tasks.register<JavaExec>("runFlink") {
    description = "Run the Flink job locally"
    mainClass.set("com.ecommerce.analytics.RealTimeAnalytics")
    classpath = sourceSets["main"].runtimeClasspath
    // Add program arguments here if needed
    args = listOf("--bootstrap-servers=localhost:29092", "--input-topic=uk-retail-raw", "--output-topic=uk-retail-processed")
    // System properties can be added here if needed
    systemProperty("log4j.configurationFile", "log4j2.properties")
}

// Configure compile tasks for better reporting
tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs.add("-Xlint:deprecation")
    options.compilerArgs.add("-Xlint:unchecked")
}

// Package source code
tasks.register<Jar>("sourcesJar") {
    archiveClassifier.set("sources")
    from(sourceSets.main.get().allJava)
}