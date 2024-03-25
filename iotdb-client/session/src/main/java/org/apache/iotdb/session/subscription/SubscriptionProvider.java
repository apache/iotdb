package org.apache.iotdb.session.subscription;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;

import org.apache.thrift.TException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public final class SubscriptionProvider extends SubscriptionSession {

  private final String consumerId;
  private final String consumerGroupId;

  public String getConsumerId() {
    return consumerId;
  }

  public String getConsumerGroupId() {
    return consumerGroupId;
  }

  public SubscriptionProvider(
      TEndPoint endPoint,
      String username,
      String password,
      String consumerId,
      String consumerGroupId) {
    super(endPoint.ip, String.valueOf(endPoint.port), username, password);

    this.consumerId = consumerId;
    this.consumerGroupId = consumerGroupId;
  }

  public Map<Integer, TEndPoint> handshake()
      throws IoTDBConnectionException, TException, IOException, StatementExecutionException {
    open();

    Map<String, String> consumerAttributes = new HashMap<>();
    consumerAttributes.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, consumerGroupId);
    consumerAttributes.put(ConsumerConstant.CONSUMER_ID_KEY, consumerId);
    return getSessionConnection().handshake(new ConsumerConfig(consumerAttributes));
  }

  @Override
  public void close() throws IoTDBConnectionException {
    try {
      getSessionConnection().closeConsumer();
    } catch (TException | StatementExecutionException e) {
      // wrap to IoTDBConnectionException to keep interface consistent
      throw new IoTDBConnectionException(e);
    }
    super.close();
  }

  public SubscriptionSessionConnection getSessionConnection() {
    return (SubscriptionSessionConnection) defaultSessionConnection;
  }
}
