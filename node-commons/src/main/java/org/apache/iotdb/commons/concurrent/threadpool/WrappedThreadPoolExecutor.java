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

package org.apache.iotdb.commons.concurrent.threadpool;

import org.apache.iotdb.commons.concurrent.IoTThreadFactory;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.service.JMXService;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class WrappedThreadPoolExecutor extends ThreadPoolExecutor
    implements WrappedThreadPoolExecutorMBean {
  private final String mbeanName;

  public WrappedThreadPoolExecutor(
      int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue,
      ThreadFactory threadFactory,
      String mbeanName) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    this.mbeanName =
        String.format(
            "%s:%s=%s", IoTDBConstant.IOTDB_THREADPOOL_PACKAGE, IoTDBConstant.JMX_TYPE, mbeanName);
    JMXService.registerMBean(this, this.mbeanName);
  }

  public WrappedThreadPoolExecutor(
      int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue,
      IoTThreadFactory ioTThreadFactory,
      String mbeanName) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, ioTThreadFactory);
    this.mbeanName =
        String.format(
            "%s:%s=%s", IoTDBConstant.IOTDB_THREADPOOL_PACKAGE, IoTDBConstant.JMX_TYPE, mbeanName);
    JMXService.registerMBean(this, this.mbeanName);
  }

  @Override
  public void shutdown() {
    super.shutdown();
    JMXService.deregisterMBean(mbeanName);
  }

  @Override
  public List<Runnable> shutdownNow() {
    JMXService.deregisterMBean(mbeanName);
    return super.shutdownNow();
  }

  @Override
  public int getQueueLength() {
    return getQueue().size();
  }
}
