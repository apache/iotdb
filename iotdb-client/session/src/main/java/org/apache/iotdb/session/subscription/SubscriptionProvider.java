package org.apache.iotdb.session.subscription;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;

import org.apache.thrift.TException;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

final class SubscriptionProvider extends SubscriptionSession {

  private final String consumerId;
  private final String consumerGroupId;

  private boolean isClosed = true;

  SubscriptionProvider(
      TEndPoint endPoint,
      String username,
      String password,
      String consumerId,
      String consumerGroupId) {
    super(endPoint.ip, String.valueOf(endPoint.port), username, password);

    this.consumerId = consumerId;
    this.consumerGroupId = consumerGroupId;
  }

  Map<Integer, TEndPoint> handshake()
      throws IoTDBConnectionException, TException, IOException, StatementExecutionException {
    if (!isClosed) {
      return Collections.emptyMap();
    }

    super.open();

    Map<String, String> consumerAttributes = new HashMap<>();
    consumerAttributes.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, consumerGroupId);
    consumerAttributes.put(ConsumerConstant.CONSUMER_ID_KEY, consumerId);
    Map<Integer, TEndPoint> endPoints =
        getSessionConnection().handshake(new ConsumerConfig(consumerAttributes));

    isClosed = false;
    return endPoints;
  }

  @Override
  public void close() throws IoTDBConnectionException {
    if (isClosed) {
      return;
    }

    try {
      getSessionConnection().closeConsumer();
    } catch (TException | StatementExecutionException e) {
      // wrap to IoTDBConnectionException to keep interface consistent
      throw new IoTDBConnectionException(e);
    } finally {
      super.close();
      isClosed = true;
    }
  }

  SubscriptionSessionConnection getSessionConnection() {
    return (SubscriptionSessionConnection) defaultSessionConnection;
  }
}
