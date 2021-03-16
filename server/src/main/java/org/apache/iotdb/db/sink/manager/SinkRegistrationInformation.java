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

package org.apache.iotdb.db.sink.manager;

import org.apache.iotdb.db.sink.api.Configuration;
import org.apache.iotdb.db.sink.api.Event;
import org.apache.iotdb.db.sink.api.Handler;

import java.util.concurrent.atomic.AtomicInteger;

public class SinkRegistrationInformation<C extends Configuration, E extends Event> {

  private final Sink sink;

  private final Configuration configuration;
  private final Handler<C, E> handler;

  private final AtomicInteger count;

  public SinkRegistrationInformation(
      Sink sink, Configuration configuration, Handler<C, E> handler, AtomicInteger count) {
    this.sink = sink;
    this.configuration = configuration;
    this.handler = handler;
    this.count = count;
  }

  public int incrementAndGet() {
    return count.incrementAndGet();
  }

  public int decrementAndGet() {
    return count.decrementAndGet();
  }

  public Sink getSink() {
    return sink;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public Handler<C, E> getHandler() {
    return handler;
  }
}
