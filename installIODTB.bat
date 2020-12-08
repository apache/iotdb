mvn clean package -pl client -am -DskipTests
mvn clean package -pl server -am -DskipTests
mvn clean package -pl zeppelin-interpreter -am -DskipTests