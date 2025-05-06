

export JAVA_HOME=/usr/lib/jvm/java-11
export SPRING_CONFIG_LOCATION=file:./core/src/main/resources/config/application.yml

echo "Starting Data Lakehouse API service..."
java -jar api/target/api-1.0.0-SNAPSHOT.jar
