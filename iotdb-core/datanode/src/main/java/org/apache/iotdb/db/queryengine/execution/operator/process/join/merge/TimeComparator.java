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

package org.apache.iotdb.db.queryengine.execution.operator.process.join.merge;

public interface TimeComparator {

  /** return true if time is satisfied with endTime, otherwise false. */
  boolean satisfyCurEndTime(long time, long endTime);

  /** return min(time1, time2) if order by time asc, max(time1, time2) if order by desc. */
  long getCurrentEndTime(long time1, long time2);

  /** return time < endTime if order by time asc, time > endTime if order by desc. */
  boolean canContinue(long time, long endTime);
}
