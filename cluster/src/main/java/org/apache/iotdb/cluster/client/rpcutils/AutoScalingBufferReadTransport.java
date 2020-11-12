package org.apache.iotdb.cluster.client.rpcutils;

import org.apache.thrift.transport.AutoExpandingBufferReadTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author HouliangQi (neuyilan@163.com)
 * @description
 * @since 2020-11-11 20:08
 */
public class AutoScalingBufferReadTransport extends AutoExpandingBufferReadTransport {

  private static final Logger logger = LoggerFactory
      .getLogger(AutoScalingBufferReadTransport.class);
  private final AutoScalingBuffer buf;

  public AutoScalingBufferReadTransport(int initialCapacity, double overgrowthCoefficient) {
    super(initialCapacity, overgrowthCoefficient);
    this.buf = new AutoScalingBuffer(initialCapacity, overgrowthCoefficient);
  }

  public void shrinkSizeIfNecessary(int size) {
    logger.debug("try to shrink the read buffer, size={}", size);
    buf.shrinkSizeIfNecessary(size);
  }
}
