plugins {
    kotlin("jvm") version "1.9.21"
    id("com.google.cloud.tools.jib") version "3.3.1"
}

group = "org.example"
version = "1.0-SNAPSHOT"

val googleApiVersion: String by project
val commonsCsvVersion: String by project
val kotlinxSerializationVersion: String by project
val kotlinxCoroutinesVersion: String by project
val logbackVersion: String by project
val kotlinxDateTimeVersion: String by project

repositories {
    mavenCentral()
}

dependencies {
    implementation(platform("com.google.cloud:libraries-bom:26.11.0"))
    api("org.apache.commons:commons-csv:$commonsCsvVersion")
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:$kotlinxCoroutinesVersion")
    api("org.jetbrains.kotlinx:kotlinx-serialization-json:$kotlinxSerializationVersion")
    api("com.google.cloud:google-cloud-storage:")
    api( "ch.qos.logback:logback-core:$logbackVersion")
    api( "ch.qos.logback:logback-classic:$logbackVersion")
    api( "com.google.cloud:google-cloud-logging-logback:0.131.0-alpha")
    api("org.jetbrains.kotlinx:kotlinx-datetime:$kotlinxDateTimeVersion")
    api("com.google.cloud:google-cloud-bigquery:")

    testImplementation("org.jetbrains.kotlin:kotlin-test")
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(17)
}

jib {
    to {
        image="europe-west3-docker.pkg.dev/data-ingest-421014/d2v-docker/data-ingest:$version"
    }
}