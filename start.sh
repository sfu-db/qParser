#!/bin/bash
set -e

# Fix JAVA_HOME for Maven
export JAVA_HOME=/opt/java/openjdk
export PATH=$JAVA_HOME/bin:$PATH

APP_PORT=${APP_PORT:-25333}

echo "Building Maven project..."
mvn clean package assembly:single -DskipTests

JAR=$(ls target/*-jar-with-dependencies.jar | head -n 1)

if [ -z "$JAR" ]; then
  echo "Error: JAR not found in target/"
  exit 1
fi

echo "Starting GatewayServer on port $APP_PORT..."

# JVM options
JVM_OPTIONS="--add-opens java.base/java.lang=ALL-UNNAMED \
	             --add-opens java.base/java.lang.invoke=ALL-UNNAMED \
		                  --add-opens java.base/java.lang.reflect=ALL-UNNAMED \
				               -Xmx1024m"  # You can also include memory options, debugging, etc.

# Run the JAR file with the JVM options

java  $JVM_OPTIONS -cp "$JAR" com.parseval.Main "$APP_PORT"