package org.apache.iotdb.session.subscription;

import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.payload.EnrichedTablets;
import org.apache.iotdb.session.Session;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class SubscriptionSession extends Session {

  public SubscriptionSession(String host, int rpcPort) {
    super(host, rpcPort);
  }

  public SubscriptionSession(String host, String rpcPort, String username, String password) {
    super(host, rpcPort, username, password);
  }

  /////////////////////////////// topic ///////////////////////////////

  public Topic createTopic(String topicName, Properties properties) {}

  public void drop(String topicName) {}

  public Set<Topic> getTopics() {}

  public Topic getTopic(String topicName) {}

  /////////////////////////////// subscription ///////////////////////////////

  public Set<Subscription> subscription() {}

  public Set<Subscription> subscription(String topicName) {}

  /////////////////////////////// consumer ///////////////////////////////

  public void createConsumer(ConsumerConfig consumerConfig) throws Exception {
    defaultSessionConnection.createConsumer(consumerConfig);
  }

  public void dropConsumer() throws Exception {
    defaultSessionConnection.dropConsumer();
  }

  public void subscribe(Set<String> topicNames) throws Exception {
    defaultSessionConnection.subscribe(topicNames);
  }

  public void unsubscribe(Set<String> topicNames) throws Exception {
    defaultSessionConnection.unsubscribe(topicNames);
  }

  public List<EnrichedTablets> poll(Set<String> topicNames) throws Exception {
    return defaultSessionConnection.poll(topicNames);
  }

  public void commit(Map<String, List<String>> topicNameToSubscriptionCommitIds) throws Exception {
    defaultSessionConnection.commit(topicNameToSubscriptionCommitIds);
  }
}
