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

package org.apache.iotdb.metrics.dropwizard.type;

import org.apache.iotdb.metrics.type.AutoGauge;

import java.lang.ref.WeakReference;
import java.util.function.ToLongFunction;

public class DropwizardAutoGauge<T> implements AutoGauge, com.codahale.metrics.Gauge<Long> {

  private final WeakReference<T> refObject;
  private final ToLongFunction<T> mapper;

  public DropwizardAutoGauge(T obj, ToLongFunction<T> mapper) {
    this.refObject = new WeakReference<>(obj);
    this.mapper = mapper;
  }

  @Override
  public Long getValue() {
    if (refObject.get() == null) {
      return 0L;
    }
    return mapper.applyAsLong(refObject.get());
  }

  @Override
  public long value() {
    return getValue();
  }
}
