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

package org.apache.iotdb.tsfile.read.common;

import java.util.function.Predicate;

public interface IBatchDataIterator {

  boolean hasNext();

  boolean hasNext(long minBound, long maxBound);

  /**
   * Determine whether there is a non-null value in the current time window. This method is used in
   * GROUP BY aggregation query.
   *
   * @param boundPredicate A predicate used to judge whether the current timestamp is out of time
   *     range, returns true if it is. This predicate guarantees that the current time of batchData
   *     will not out of time range when a sensor's values are all null.
   */
  boolean hasNext(Predicate<Long> boundPredicate);

  void next();

  long currentTime();

  Object currentValue();

  void reset();

  int totalLength();
}
