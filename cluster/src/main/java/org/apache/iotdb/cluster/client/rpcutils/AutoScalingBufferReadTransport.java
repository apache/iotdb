package org.apache.iotdb.cluster.client.rpcutils;

import org.apache.thrift.transport.AutoExpandingBufferReadTransport;

/**
 * @author HouliangQi (neuyilan@163.com)
 * @description
 * @since 2020-11-11 20:08
 */
public class AutoScalingBufferReadTransport extends AutoExpandingBufferReadTransport {

  private final AutoScalingBuffer buf;

  public AutoScalingBufferReadTransport(int initialCapacity, double overgrowthCoefficient) {
    super(initialCapacity, overgrowthCoefficient);
    this.buf = new AutoScalingBuffer(initialCapacity, overgrowthCoefficient);
  }

  public void shrinkSizeIfNecessary(int size) {
    buf.shrinkSizeIfNecessary(size);
  }
}
