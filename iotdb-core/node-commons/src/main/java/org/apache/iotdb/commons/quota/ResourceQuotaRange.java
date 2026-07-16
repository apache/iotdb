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

package org.apache.iotdb.commons.quota;

import org.apache.iotdb.commons.conf.IoTDBConstant;

import java.util.Objects;

public class ResourceQuotaRange {

  private long minValue = IoTDBConstant.UNLIMITED_VALUE;
  private long maxValue = IoTDBConstant.UNLIMITED_VALUE;

  public ResourceQuotaRange() {}

  public ResourceQuotaRange(long minValue, long maxValue) {
    this.minValue = minValue;
    this.maxValue = maxValue;
  }

  public long getMinValue() {
    return minValue;
  }

  public void setMinValue(long minValue) {
    this.minValue = minValue;
  }

  public long getMaxValue() {
    return maxValue;
  }

  public void setMaxValue(long maxValue) {
    this.maxValue = maxValue;
  }

  public boolean isUnlimited() {
    return minValue == IoTDBConstant.UNLIMITED_VALUE && maxValue == IoTDBConstant.UNLIMITED_VALUE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ResourceQuotaRange that = (ResourceQuotaRange) o;
    return minValue == that.minValue && maxValue == that.maxValue;
  }

  @Override
  public int hashCode() {
    return Objects.hash(minValue, maxValue);
  }
}
