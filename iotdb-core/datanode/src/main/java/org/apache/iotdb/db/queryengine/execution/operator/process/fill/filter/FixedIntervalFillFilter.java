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

package org.apache.iotdb.db.queryengine.execution.operator.process.fill.filter;

import org.apache.iotdb.db.queryengine.execution.operator.process.fill.IFillFilter;

public class FixedIntervalFillFilter implements IFillFilter {

  // the time precision of this field is same as the system time_precision configuration.
  private final long timeInterval;

  public FixedIntervalFillFilter(long timeInterval) {
    this.timeInterval = timeInterval;
  }

  @Override
  public boolean needFill(long time, long previousTime) {
    // the reason that we use Math.abs is that we may use order by time desc which will cause
    // previousTime is larger than time
    return Math.abs(time - previousTime) <= timeInterval;
  }
}
