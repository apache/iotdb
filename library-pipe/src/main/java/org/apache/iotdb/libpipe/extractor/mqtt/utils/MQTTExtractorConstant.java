package org.apache.iotdb.libpipe.extractor.mqtt.utils;

public class MQTTExtractorConstant {
  public static final String MQTT_BROKER_HOST_KEY = "mqtt.host";
  public static final String MQTT_BROKER_HOST_DEFAULT_VALUE = "127.0.0.1";
  public static final String MQTT_BROKER_PORT_KEY = "mqtt.port";
  public static final String MQTT_BROKER_PORT_DEFAULT_VALUE = "1883";

  public static final String MQTT_BROKER_INTERCEPTOR_THREAD_POOL_SIZE_KEY = "mqtt.pool-size";
  public static final int MQTT_BROKER_INTERCEPTOR_THREAD_POOL_SIZE_DEFAULT_VALUE = 1;

  public static final String MQTT_DATA_PATH_PROPERTY_NAME_KEY = "mqtt.data-path";
  public static final String MQTT_DATA_PATH_PROPERTY_NAME_DEFAULT_VALUE = "data/";

  public static final String MQTT_IMMEDIATE_BUFFER_FLUSH_PROPERTY_NAME_KEY = "mqtt.immediate-flush";
  public static final boolean MQTT_IMMEDIATE_BUFFER_FLUSH_PROPERTY_NAME_DEFAULT_VALUE = true;

  public static final String MQTT_ALLOW_ANONYMOUS_PROPERTY_NAME_KEY = "mqtt.allow-anonymous";
  public static final boolean MQTT_ALLOW_ANONYMOUS_PROPERTY_NAME_DEFAULT_VALUE = false;

  public static final String MQTT_ALLOW_ZERO_BYTE_CLIENT_ID_PROPERTY_NAME_KEY =
      "mqtt.allow-zero-byte-client-id";
  public static final boolean MQTT_ALLOW_ZERO_BYTE_CLIENT_ID_PROPERTY_NAME_DEFAULT_VALUE = true;

  public static final String MQTT_NETTY_MAX_BYTES_PROPERTY_NAME_KEY = "mqtt.max-message-size";
  public static final long MQTT_NETTY_MAX_BYTES_PROPERTY_NAME_DEFAULT_VALUE = 1048576;

  public static final String MQTT_PAYLOAD_FORMATTER_KEY = "mqtt.payload-formatter";
  public static final String MQTT_PAYLOAD_FORMATTER_DEFAULT_VALUE = "json";
}
