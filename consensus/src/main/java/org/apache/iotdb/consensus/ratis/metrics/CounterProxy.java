/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.consensus.ratis.metrics;

import org.apache.iotdb.metrics.type.Counter;

/** A Proxy class using IoTDB Counter to replace the dropwizard Counter. */
public class CounterProxy extends com.codahale.metrics.Counter {

  /** IoTDB Counter */
  private final Counter counter;

  CounterProxy(Counter counter) {
    this.counter = counter;
  }

  @Override
  public void inc() {
    inc(1L);
  }

  @Override
  public void inc(long n) {
    counter.inc(n);
  }

  @Override
  public void dec() {
    inc(-1L);
  }

  @Override
  public void dec(long n) {
    inc(-n);
  }

  @Override
  public long getCount() {
    return counter.count();
  }
}
