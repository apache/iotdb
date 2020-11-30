/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.cluster.client.rpcutils;

import org.apache.thrift.transport.AutoExpandingBufferReadTransport;

public class AutoScalingBufferReadTransport extends AutoExpandingBufferReadTransport {

  private final AutoScalingBuffer buf;

  public AutoScalingBufferReadTransport(int initialCapacity, double overgrowthCoefficient) {
    super(initialCapacity, overgrowthCoefficient);
    this.buf = new AutoScalingBuffer(initialCapacity, overgrowthCoefficient);
  }

  /**
   * shrink the buffer to the specific size
   *
   * @param size The size of the target you want to shrink to
   */
  public void shrinkSizeIfNecessary(int size) {
    buf.shrinkSizeIfNecessary(size);
  }

  public void expandIfNecessary(int size) {
    buf.resizeIfNecessary(size);
  }
}
