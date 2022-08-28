package org.apache.iotdb.pipe.external.kafka;

import org.apache.iotdb.session.pool.SessionPool;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.iotdb.pipe.external.kafka.KafkaConstant.MAX_CONSUMERS;

public class KafkaLoader {
  private Long startTime;
  private LoaderStatus status = null;
  private ExecutorService executor;
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
    if (kafkaParams.containsKey("max_consumer")) {
      this.consumer_num = (int) Math.floor(Double.parseDouble(kafkaParams.get("max_consumer")));
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
      // props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 100 * 1024 * 1024);
      // props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 100 * 1024 * 1024);
      props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 600000);
      props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
      props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 60000);

      props.put(ConsumerConfig.GROUP_ID_CONFIG, topic);

      KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
      consumer.subscribe(Collections.singleton(topic));

      if (i == 0) {
        int partition_num = get_partition_num(consumer, topic);
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
    this.startTime = System.currentTimeMillis();
    this.status = LoaderStatus.STOP;
    return this.consumer_num;
  }

  public void close() {
    for (ConsumerThreadSync consumerThread : consumerThreads) {
      consumerThread.close();
    }
    this.executor.shutdown();
    this.status = LoaderStatus.DROP;
  }

  public void run() {
    this.executor = Executors.newFixedThreadPool(this.consumer_num);
    for (ConsumerThreadSync consumerThread : consumerThreads) {
      consumerThread.open();
      this.executor.submit(consumerThread);
    }
    this.status = LoaderStatus.RUNNING;
  }

  public void cancel() {
    for (ConsumerThreadSync consumerThread : consumerThreads) {
      consumerThread.pause();
    }
    this.executor.shutdown();
    this.status = LoaderStatus.STOP;
  }

  public static int get_partition_num(KafkaConsumer<String, String> consumer, String topic) {
    return consumer.listTopics().get(topic).size();
  }

  enum LoaderStatus {
    RUNNING,
    STOP,
    DROP
  }

  public LoaderStatus getStatus() {
    return this.status;
  }

  @Override
  public String toString() {
    Long numOfSuccessfulInsertion = 0L;
    Long numOfSuccessfulStringInsertion = 0L;
    Long numOfSuccessfulRecordDeletion = 0L;
    Long numOfSuccessfulTimeSeriesDeletion = 0L;
    Long numOfFailedInsertion = 0L;
    Long numOfFailedStringInsertion = 0L;
    Long numOfFailedRecordDeletion = 0L;
    Long numOfFailedTimeSeriesDeletion = 0L;

    for (ConsumerThreadSync consumerThread : consumerThreads) {
      ConsumerSyncStatus cs = consumerThread.consumerSyncStatus;
      numOfSuccessfulInsertion += cs.getNumOfSuccessfulInsertion();
      numOfSuccessfulStringInsertion += cs.getNumOfSuccessfulStringInsertion();
      numOfSuccessfulRecordDeletion += cs.getNumOfSuccessfulRecordDeletion();
      numOfSuccessfulTimeSeriesDeletion += cs.getNumOfSuccessfulTimeSeriesDeletion();
      numOfFailedInsertion += cs.getNumOfFailedInsertion();
      numOfFailedStringInsertion += cs.getNumOfFailedStringInsertion();
      numOfFailedRecordDeletion += cs.getNumOfFailedRecordDeletion();
      numOfFailedTimeSeriesDeletion += cs.getNumOfFailedTimeSeriesDeletion();
    }

    return "LoaderSyncStatus{"
        + "startTime="
        + startTime
        + ", numOfSuccessfulInsertion="
        + numOfSuccessfulInsertion
        + ", numOfSuccessfulStringInsertion="
        + numOfSuccessfulStringInsertion
        + ", numOfSuccessfulRecordDeletion="
        + numOfSuccessfulRecordDeletion
        + ", numOfSuccessfulTimeSeriesDeletion="
        + numOfSuccessfulTimeSeriesDeletion
        + ", numOfFailedInsertion="
        + numOfFailedInsertion
        + ", numOfFailedStringInsertion="
        + numOfFailedStringInsertion
        + ", numOfFailedRecordDeletion="
        + numOfFailedRecordDeletion
        + ", numOfFailedTimeSeriesDeletion="
        + numOfFailedTimeSeriesDeletion
        + '}';
  }
}
