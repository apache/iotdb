1/*
1 * Licensed to the Apache Software Foundation (ASF) under one
1 * or more contributor license agreements.  See the NOTICE file
1 * distributed with this work for additional information
1 * regarding copyright ownership.  The ASF licenses this file
1 * to you under the Apache License, Version 2.0 (the
1 * "License"); you may not use this file except in compliance
1 * with the License.  You may obtain a copy of the License at
1 *
1 *     http://www.apache.org/licenses/LICENSE-2.0
1 *
1 * Unless required by applicable law or agreed to in writing,
1 * software distributed under the License is distributed on an
1 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1 * KIND, either express or implied.  See the License for the
1 * specific language governing permissions and limitations
1 * under the License.
1 */
1
1package org.apache.iotdb.confignode.procedure;
1
1import org.slf4j.Logger;
1import org.slf4j.LoggerFactory;
1
1public abstract class StoppableThread extends Thread {
1
1  private static final int JOIN_TIMEOUT = 250;
1  private static final Logger LOG = LoggerFactory.getLogger(StoppableThread.class);
1
1  protected StoppableThread(ThreadGroup threadGroup, String name) {
1    super(threadGroup, name);
1  }
1
1  public abstract void sendStopSignal();
1
1  public void awaitTermination() {
1    try {
1      for (int i = 0; isAlive(); i++) {
1        sendStopSignal();
1        join(JOIN_TIMEOUT);
1        if (i > 0 && (i % 8) == 0) {
1          interrupt();
1        }
1      }
1    } catch (InterruptedException e) {
1      LOG.warn("{} join wait got interrupted", getName(), e);
1      Thread.currentThread().interrupt();
1    }
1  }
1
1  @Override
1  public void run() {
1    // empty method
1  }
1}
1