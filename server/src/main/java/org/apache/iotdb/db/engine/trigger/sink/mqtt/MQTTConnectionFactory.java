package org.apache.iotdb.db.engine.trigger.sink.mqtt;

import org.apache.iotdb.db.engine.trigger.sink.exception.SinkException;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;

public class MQTTConnectionFactory extends BasePooledObjectFactory<BlockingConnection> {
  private final String host;
  private final int port;
  private final String username;
  private final String password;
  private final long connectAttemptsMax;
  private final long reconnectDelay;

  public MQTTConnectionFactory(
      String host,
      int port,
      String username,
      String password,
      long connectAttemptsMax,
      long reconnectDelay) {
    this.host = host;
    this.port = port;
    this.username = username;
    this.password = password;
    this.connectAttemptsMax = connectAttemptsMax;
    this.reconnectDelay = reconnectDelay;
  }

  @Override
  public BlockingConnection create() throws Exception {
    MQTT mqtt = new MQTT();
    mqtt.setHost(host, port);
    mqtt.setUserName(username);
    mqtt.setPassword(password);
    mqtt.setConnectAttemptsMax(connectAttemptsMax);
    mqtt.setReconnectDelay(reconnectDelay);

    return mqtt.blockingConnection();
  }

  @Override
  public PooledObject<BlockingConnection> wrap(BlockingConnection blockingConnection) {
    return new DefaultPooledObject<>(blockingConnection);
  }

  @Override
  public void activateObject(PooledObject<BlockingConnection> p) throws Exception {
    if (p == null) {
      return;
    }
    BlockingConnection connection = p.getObject();
    try {
      if (!connection.isConnected()) {
        connection.connect();
      }
    } catch (Exception e) {
      if (connection.isConnected()) {
        connection.resume();
      }
      connection.kill();
      throw new SinkException("MQTT Connection activate error", e);
    }
    super.activateObject(p);
  }

  @Override
  public void passivateObject(PooledObject<BlockingConnection> p) throws Exception {
    if (p == null) {
      return;
    }
    BlockingConnection connection = p.getObject();
    try {
      if (connection != null && connection.isConnected()) {
        connection.resume();
      }
    } catch (Exception e) {
      throw new SinkException("MQTT connection passivate error", e);
    }
    super.passivateObject(p);
  }

  @Override
  public boolean validateObject(PooledObject<BlockingConnection> p) {
    if (p == null) {
      return false;
    }
    BlockingConnection connection = p.getObject();
    return connection != null && connection.isConnected();
  }

  @Override
  public void destroyObject(PooledObject<BlockingConnection> p) throws Exception {
    if (p == null) {
      return;
    }
    BlockingConnection connection = p.getObject();
    try {
      if (connection != null) {
        if (connection.isConnected()) {
          connection.disconnect();
        }
        connection.kill();
      }
    } catch (Exception e) {
      throw new SinkException("MQTT connection destroy error", e);
    }
    super.destroyObject(p);
  }
}
