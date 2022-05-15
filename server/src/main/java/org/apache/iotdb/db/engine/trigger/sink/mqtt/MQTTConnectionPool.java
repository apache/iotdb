package org.apache.iotdb.db.engine.trigger.sink.mqtt;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.QoS;

public class MQTTConnectionPool extends GenericObjectPool<BlockingConnection> {

  public MQTTConnectionPool(MQTTConnectionFactory factory, int size) {
    super(factory);
    setMaxTotal(size);
    setMinIdle(1);
  }

  public void connect() throws Exception {
    BlockingConnection connection = borrowObject();
    if (!connection.isConnected()) {
      connection.connect();
    }
    returnObject(connection);
  }

  public void disconnectAndClose() {
    clear();
    close();
  }

  public void publish(final String topic, final byte[] payload, final QoS qos, final boolean retain)
      throws Exception {
    BlockingConnection connection = this.borrowObject();
    connection.publish(topic, payload, qos, retain);
    returnObject(connection);
  }
}
