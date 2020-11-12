package org.apache.iotdb.cluster.client.rpcutils;

import java.util.Arrays;
import org.apache.thrift.transport.AutoExpandingBuffer;

/**
 * @author HouliangQi (neuyilan@163.com)
 * @description
 * @since 2020-11-11 20:20
 */
public class AutoScalingBuffer extends AutoExpandingBuffer {

  private byte[] array;

  public AutoScalingBuffer(int initialCapacity, double growthCoefficient) {
    super(initialCapacity, growthCoefficient);
    this.array = new byte[initialCapacity];
  }

  public void shrinkSizeIfNecessary(int size) {
    if (array.length > size) {
      array = Arrays.copyOf(array, size);
    }
  }
}
