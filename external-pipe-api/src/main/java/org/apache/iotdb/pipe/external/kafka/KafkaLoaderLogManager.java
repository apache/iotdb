package org.apache.iotdb.pipe.external.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;

import static org.apache.iotdb.pipe.external.kafka.KafkaConstant.LOG_DIR;
import static org.apache.iotdb.pipe.external.kafka.KafkaConstant.MAX_CONSUMERS;

public class KafkaLoaderLogManager {
  private final String offset_filename;
  private DataOutputStream offset_outputStream;
  private final String topic;
  private final KafkaConsumer<String, String> consumer;

  KafkaLoaderLogManager(String topic, int consumer_id, KafkaConsumer<String, String> consumer) {
    this.topic = topic;
    this.consumer = consumer;
    this.offset_filename = LOG_DIR + "/" + topic + "_" + consumer_id + "_offset.log";
  }

  public boolean init() throws IOException {
    File newOffsetLog = new File(this.offset_filename);
    boolean no_offset = createFile(newOffsetLog);
    this.offset_outputStream = new DataOutputStream(Files.newOutputStream(newOffsetLog.toPath()));
    return no_offset;
  }

  public static boolean createFile(File file) throws IOException {
    if (!file.getParentFile().exists()) {
      file.getParentFile().mkdirs();
    }
    return file.createNewFile();
  }

  public void writeOffset() throws IOException {
    for (PartitionInfo partition_info : this.consumer.partitionsFor(topic)) {
      this.write_single_offset(
          partition_info.partition(),
          this.consumer.position(new TopicPartition(topic, partition_info.partition())));
    }
    this.offset_outputStream.flush();
  }

  private void write_single_offset(int partition, long position) throws IOException {
    this.offset_outputStream.writeByte(partition);
    this.offset_outputStream.writeLong(position);
  }

  public void reset_consumer() throws IOException {
    try (RandomAccessFile rf = new RandomAccessFile(this.offset_filename, "r")) {
      long len = rf.length();
      long start = rf.getFilePointer();
      long end = start + len - 1;
      rf.seek(end);
      int c;
      while (end > start) {
        c = rf.read();
        if (c == '\n' || c == '\r') {
          reset_with_line(rf);
          break;
        }
        end--;
        rf.seek(end);
        if (end == 0) {
          reset_with_line(rf);
        }
      }
    } catch (Exception e) {
      throw e;
    }
  }

  private void reset_with_line(RandomAccessFile rf) throws IOException {
    String line = rf.readLine();
    long[] offsets = new long[MAX_CONSUMERS];
    List<TopicPartition> partitionList = null;
    int length = line.length();
    int num = 0;
    int partition_num = 0;
    while (num < length - 1) {
      partitionList.add(new TopicPartition(this.topic, line.charAt(num)));
      num += Byte.BYTES;
      offsets[partition_num] = Long.parseLong(line.substring(num, num + Long.BYTES));
      num += Long.BYTES;
      partition_num += 1;
    }
    this.consumer.assign(partitionList);
    this.consumer.poll(Duration.ofSeconds(0));
    for (int i = 0; i < partition_num; ++i) {
      this.consumer.seek(partitionList.get(i), offsets[i]);
    }
  }
}
