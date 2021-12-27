FROM openjdk:8-jre
ADD target/m12_kafkastreams_jvm_azure-1.0-SNAPSHOT-jar-with-dependencies.jar app.jar
ENTRYPOINT ["java","-cp","/app.jar","com.epam.bd201.KStreamsApplication"]
