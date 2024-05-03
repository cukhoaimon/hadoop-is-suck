plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-client-core
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:3.3.6")
    // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
    implementation("org.apache.hadoop:hadoop-common:3.3.6")

    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}