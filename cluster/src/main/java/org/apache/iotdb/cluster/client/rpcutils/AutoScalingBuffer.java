package org.apache.iotdb.cluster.client.rpcutils;

import java.util.Arrays;
import org.apache.thrift.transport.AutoExpandingBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author HouliangQi (neuyilan@163.com)
 * @description
 * @since 2020-11-11 20:20
 */
public class AutoScalingBuffer extends AutoExpandingBuffer {

  private static final Logger logger = LoggerFactory.getLogger(AutoScalingBuffer.class);

  private byte[] array;

  public AutoScalingBuffer(int initialCapacity, double growthCoefficient) {
    super(initialCapacity, growthCoefficient);
    this.array = new byte[initialCapacity];
  }

  public void shrinkSizeIfNecessary(int size) {
    if (array.length > size) {
      logger.info("shrink the buffer size from {} to {}", array.length, size);
      array = Arrays.copyOf(array, size);
    }
  }
}
