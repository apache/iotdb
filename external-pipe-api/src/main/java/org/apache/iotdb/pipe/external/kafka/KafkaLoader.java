package org.apache.iotdb.pipe.external.kafka;

import org.apache.iotdb.session.pool.SessionPool;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.iotdb.pipe.external.kafka.KafkaConstant.MAX_CONSUMERS;

public class KafkaLoader {
  private ExecutorService executor;
  private static final Logger logger = LoggerFactory.getLogger(KafkaLoader.class);
  private List<KafkaConsumer<String, String>> consumerList = null;
  private final SessionPool pool;
  private int consumer_num = MAX_CONSUMERS;
  private boolean single_partition = true;
  private final Map<String, String> kafkaParams;
  private List<ConsumerThreadSync> consumerThreads = null;

  public KafkaLoader(SessionPool pool, Map<String, String> kafkaParams) {
    this.pool = pool;
    this.kafkaParams = kafkaParams;
    if (!kafkaParams.containsKey("offset")) {
      kafkaParams.put("offset", "earliest");
    }
  }

  public int open() {
    if (this.consumerList != null) {
      return 0;
    }
    this.consumerList = new ArrayList<>();
    this.consumerThreads = new ArrayList<>();
    for (int i = 0; i < consumer_num; i++) {
      String topic = this.kafkaParams.get("topic");
      Properties props = new Properties();

      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaParams.get("brokers"));

      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, this.kafkaParams.get("offset"));

      props.put(ConsumerConfig.GROUP_ID_CONFIG, topic);

      KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
      consumer.subscribe(Collections.singleton(topic));

      if (i == 0) {
        int partition_num = get_partition_num(consumer, this.kafkaParams.get("topic"));
        if (partition_num > 1) {
          this.single_partition = false;
        }
        if (this.consumer_num > partition_num) {
          this.consumer_num = partition_num;
        }
      }
      this.consumerList.add(consumer);
      this.consumerThreads.add(new ConsumerThreadSync(consumer, this.pool, this.single_partition));
    }
    return this.consumer_num;
  }

  public void close() {
    for (ConsumerThreadSync consumerThread : consumerThreads) {
      consumerThread.close();
    }
    this.executor.shutdown();
  }

  public void run() {
    this.executor = Executors.newFixedThreadPool(this.consumer_num);
    for (ConsumerThreadSync consumerThread : consumerThreads) {
      consumerThread.open();
      this.executor.submit(consumerThread);
    }
  }

  public void cancel() {
    for (ConsumerThreadSync consumerThread : consumerThreads) {
      consumerThread.pause();
    }
    this.executor.shutdown();
  }

  public static int get_partition_num(KafkaConsumer<String, String> consumer, String topic) {
    return consumer.listTopics().get(topic).size();
  }
}
