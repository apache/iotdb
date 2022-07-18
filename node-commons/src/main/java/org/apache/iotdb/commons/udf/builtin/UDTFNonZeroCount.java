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

package org.apache.iotdb.commons.udf.builtin;

public class UDTFNonZeroCount extends UDTFContinuouslySatisfy {

  @Override
  protected long getDefaultMin() {
    return 1;
  }

  @Override
  protected long getDefaultMax() {
    return Long.MAX_VALUE;
  }

  @Override
  protected Long getRecord() {
    return satisfyValueCount;
  }

  @Override
  protected boolean satisfyInt(int value) {
    return value != 0;
  }

  @Override
  protected boolean satisfyLong(long value) {
    return value != 0L;
  }

  @Override
  protected boolean satisfyFloat(float value) {
    return value != 0f;
  }

  @Override
  protected boolean satisfyDouble(double value) {
    return value != 0.0;
  }

  @Override
  protected boolean satisfyBoolean(Boolean value) {
    return value;
  }
}
