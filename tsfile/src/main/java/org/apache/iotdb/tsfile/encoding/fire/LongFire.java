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

package org.apache.iotdb.tsfile.encoding.fire;

public class LongFire extends Fire<Long> {
  public LongFire(int learning_rate) {
    super(learning_rate);
    bitWidth = 16;
    accumulator = 0;
    delta = 0L;
  }

  public void reset() {
    accumulator = 0;
    delta = 0L;
  }

  @Override
  public Long predict(Long value) {
    long alpha = accumulator >> learnShift;
    long diff = (alpha * delta) >> bitWidth;
    return value + diff;
  }

  @Override
  public void train(Long pre, Long val, Long err) {
    long gradient = err > 0 ? -delta : delta;
    accumulator -= gradient;
    delta = val - pre;
  }
}
