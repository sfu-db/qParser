#!/bin/bash

# Define the path to the JAR file
JAR_FILE="./target/com.sql.parser-1.0-SNAPSHOT-jar-with-dependencies.jar"

# JVM options
JVM_OPTIONS="--add-opens java.base/java.lang=ALL-UNNAMED \
	             --add-opens java.base/java.lang.invoke=ALL-UNNAMED \
		                  --add-opens java.base/java.lang.reflect=ALL-UNNAMED \
				               -Xmx1024m"  # You can also include memory options, debugging, etc.

# Run the JAR file with the JVM options
java $JVM_OPTIONS -jar $JAR_FILE
