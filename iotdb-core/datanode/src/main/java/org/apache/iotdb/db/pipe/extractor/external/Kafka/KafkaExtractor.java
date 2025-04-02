package org.apache.iotdb.db.pipe.extractor.external.Kafka;

import org.apache.iotdb.commons.consensus.index.impl.KafkaProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.extractor.external.PipeExternalExtractor;
import org.apache.iotdb.db.pipe.metric.source.PipeDataRegionEventCounter;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.processor.internals.PartitionGroup;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class KafkaExtractor implements PipeExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaExtractor.class);

  protected String pipeName;
  protected long creationTime;
  protected PipeTaskMeta pipeTaskMeta;
  protected final UnboundedBlockingPendingQueue<EnrichedEvent> pendingQueue =
      new UnboundedBlockingPendingQueue<>(new PipeDataRegionEventCounter());

  // Kafka related configuration
  private String topic;
  private int partitionGroupId;
  private int totalPartitionGroups;
  private int parallelism;
  private KafkaConsumer<String, String> consumer;
  private Future<?> consumerTaskFuture;
  private volatile boolean running = false;
  protected List<TopicPartition> assignedPartitions;
  protected final AtomicBoolean isClosed = new AtomicBoolean(false);
  private Thread consumerThread;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    // Add validation if required
  }

  @Override
  public void customize(PipeParameters parameters, PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    // Read runtime environment if needed.
    final PipeTaskExtractorRuntimeEnvironment environment =
        (PipeTaskExtractorRuntimeEnvironment) configuration.getRuntimeEnvironment();
    pipeName = environment.getPipeName();
    creationTime = environment.getCreationTime();
    pipeTaskMeta = environment.getPipeTaskMeta();

    // Read Kafka parameters from pipe parameters
    topic = parameters.getStringOrDefault("topic", "default_topic");
    parallelism = parameters.getIntOrDefault("parallelism", 1);
    String bootstrapServers = parameters.getStringOrDefault("bootstrapServers", "127.0.0.1:9092");
    partitionGroupId = -environment.getRegionId() - 1;


    // Create Kafka Consumer properties
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(
        ConsumerConfig.GROUP_ID_CONFIG,
        pipeName + creationTime + IoTDBDescriptor.getInstance().getConfig().getClusterId());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    // Disable auto commit for more controlled processing
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    consumer = new KafkaConsumer<>(props);
  }

  @Override
  public void start() throws Exception {
    if (consumer == null) {
      throw new IllegalStateException("Customize must be called before start");
    }

    running = true;

    // Fetch partitions for the topic and filter based on partitionGroupId/totalPartitionGroups
    if (pipeTaskMeta.getProgressIndex() == MinimumProgressIndex.INSTANCE) {
      List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
      if (partitionInfos == null || partitionInfos.isEmpty()) {
        throw new IllegalArgumentException("No partitions found for topic: " + topic);
      }
      totalPartitionGroups = partitionInfos.size();
      if (partitionGroupId >= totalPartitionGroups) {
        return;
      }
      assignedPartitions =
          partitionInfos.stream()
              .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
              .filter(tp -> tp.partition() % parallelism == partitionGroupId)
              .collect(Collectors.toList());
      consumer.assign(assignedPartitions);
      consumer.seekToBeginning(assignedPartitions);
    } else {
      // If the progress index is not MinimumProgressIndex, we use the progressIndex
      assignedPartitions = new ArrayList<>();
      List<Long> offsets = new ArrayList<>();
      ((KafkaProgressIndex) (pipeTaskMeta.getProgressIndex()))
          .getPartitionToOffset()
          .forEach(
              (partition, offset) -> {
                TopicPartition topicPartition = new TopicPartition(topic, partition);
                assignedPartitions.add(topicPartition);
                offsets.add(offset);
              });
      consumer.assign(assignedPartitions);
      for (int i = 0; i < assignedPartitions.size(); i++) {
        TopicPartition topicPartition = assignedPartitions.get(i);
        long offset = offsets.get(i);
        consumer.seek(topicPartition, offset+1);
      }
    }
    // Assign the filtered partitions manually

    LOGGER.info(
        "Assigned partitions {} to partitionGroupId {}", assignedPartitions, partitionGroupId);

    // Start background consumer thread
    consumerThread = new Thread(() -> {
      try {
        while (running) {
          ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
          records.forEach(record -> {
            EnrichedEvent event = extractEvent(record);
            if (!event.increaseReferenceCount(PipeExternalExtractor.class.getName())) {
              LOGGER.warn("The reference count of the event {} cannot be increased, skipping it.", event);
            }
            pendingQueue.waitedOffer(event);
          });
          consumer.commitAsync();
        }
      } catch (WakeupException e) {
        LOGGER.info("Kafka consumer is waking up for shutdown");
      } catch (Exception e) {
        LOGGER.error("Kafka consumer encountered error", e);
        throw e;
      }
    });
    consumerThread.setName("Kafka Consumer Thread " + topic + partitionGroupId);
    consumerThread.start();
    LOGGER.info("KafkaExtractor started successfully");
  }

  private EnrichedEvent extractEvent(ConsumerRecord<String, String> record) {
    // Create an event from the Kafka record
    String data = record.value();
    String[] dataArray = data.split(",");
    String device = dataArray[0];
    long timestamp = Long.parseLong(dataArray[1]);
    List<String> measurements = Arrays.asList(dataArray[2].split(":"));
    final MeasurementSchema[] schemas = new MeasurementSchema[measurements.size()];
    List<TSDataType> types = new ArrayList<>();
    for (String type : dataArray[3].split(":")) {
      types.add(TSDataType.valueOf(type));
    }
    String[] valuesStr = dataArray[4].split(":");
    Object[] values = new Object[valuesStr.length];
    for (int i = 0; i < valuesStr.length; i++) {
      switch (types.get(i)) {
        case INT64:
          values[i] = new long[] {Long.parseLong(valuesStr[i])};
          schemas[i] = new MeasurementSchema(measurements.get(i), TSDataType.INT64);
          break;
        case DOUBLE:
          values[i] = new double[] {Double.parseDouble(valuesStr[i])};
          schemas[i] = new MeasurementSchema(measurements.get(i), TSDataType.DOUBLE);
          break;
        case INT32:
          values[i] = new int[] {Integer.parseInt(valuesStr[i])};
          schemas[i] = new MeasurementSchema(measurements.get(i), TSDataType.INT32);
          break;
        case TEXT:
          values[i] = new String[] {valuesStr[i]};
          schemas[i] = new MeasurementSchema(measurements.get(i), TSDataType.TEXT);
          break;
        case FLOAT:
          values[i] = new float[] {Float.parseFloat(valuesStr[i])};
          schemas[i] = new MeasurementSchema(measurements.get(i), TSDataType.FLOAT);
          break;
        case BOOLEAN:
          values[i] = new boolean[] {Boolean.parseBoolean(valuesStr[i])};
          schemas[i] = new MeasurementSchema(measurements.get(i), TSDataType.BOOLEAN);
          break;
      }
    }
    BitMap[] bitMapsForInsertRowNode = new BitMap[schemas.length];

    Tablet tabletForInsertRowNode =
        new Tablet(
            device,
            Arrays.asList(schemas),
            new long[] {timestamp},
            values,
            bitMapsForInsertRowNode,
            1);

    EnrichedEvent event =
        new PipeRawTabletInsertionEvent(
            false,
            device,
            null,
            null,
            tabletForInsertRowNode,
            false,
            pipeName,
            creationTime,
            pipeTaskMeta,
            null,
            false);
    event.bindProgressIndex(new KafkaProgressIndex(record.partition(), record.offset()));
    return event;
  }

  @Override
  public Event supply() throws Exception {
    // Supply the next event from pending queue
      if (isClosed.get()) {
          return null;
      }

      return pendingQueue.directPoll();
  }

  @Override
  public void close() throws Exception {
    running = false;
    if (consumer != null) {
      consumer.wakeup();
    }
    if(consumerThread != null) {
        consumerThread.join();
    }

    if (consumer != null) {
      consumer.close();
    }
    isClosed.set(true);
    LOGGER.info("KafkaExtractor closed");
  }
}
