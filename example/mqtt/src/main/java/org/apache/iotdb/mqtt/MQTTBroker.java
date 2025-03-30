package org.apache.iotdb.mqtt;

import io.moquette.BrokerConstants;
import io.moquette.broker.Server;
import io.moquette.broker.config.MemoryConfig;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

public class MQTTBroker {
  public static void main(String[] args) throws Exception {
    Properties properties = new Properties();
    // Bind the server on all available network interfaces
    properties.setProperty(BrokerConstants.HOST_PROPERTY_NAME, "127.0.0.1");
    // Set the MQTT broker listening port
    properties.setProperty(BrokerConstants.PORT_PROPERTY_NAME, "1883");
    // Set the size of the thread pool for handling broker interceptors
    properties.setProperty(BrokerConstants.BROKER_INTERCEPTOR_THREAD_POOL_SIZE, "4");
    // Define the path to store MQTT session data or logs
    properties.setProperty(
        BrokerConstants.DATA_PATH_PROPERTY_NAME, "C:\\Users\\22503\\Desktop\\mqtt\\data");
    // Flush buffers immediately to ensure message durability
    properties.setProperty(BrokerConstants.IMMEDIATE_BUFFER_FLUSH_PROPERTY_NAME, "true");
    // Disable anonymous client connections for security
    properties.setProperty(BrokerConstants.ALLOW_ANONYMOUS_PROPERTY_NAME, "false");
    // Allow clients with zero-length client IDs if needed
    properties.setProperty(BrokerConstants.ALLOW_ZERO_BYTE_CLIENT_ID_PROPERTY_NAME, "true");
    // Set maximum allowed message size (e.g. 10MB)
    properties.setProperty(
        BrokerConstants.NETTY_MAX_BYTES_PROPERTY_NAME, String.valueOf(10 * 1024 * 1024));
    Server mqttBroker = new Server();
    mqttBroker.startServer(new MemoryConfig(properties));
    System.out.println("MQTT Broker started...");

    // Adding a shutdown hook to ensure proper shutdown when the JVM exits.
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  System.out.println("Shutting down MQTT Broker...");
                  mqttBroker.stopServer();
                  System.out.println("MQTT Broker stopped.");
                }));

    // Waiting for user input to shutdown the broker.
    System.out.println("Press ENTER to stop the broker.");
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    reader.readLine();

    // Manually stopping the broker.
    mqttBroker.stopServer();
    System.out.println("MQTT Broker stopped.");
  }
}
