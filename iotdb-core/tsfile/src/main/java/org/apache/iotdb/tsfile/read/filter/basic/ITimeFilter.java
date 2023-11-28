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

package org.apache.iotdb.tsfile.read.filter.basic;

import org.apache.iotdb.tsfile.file.metadata.IAlignedMetadataProvider;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;

import java.io.Serializable;

public interface ITimeFilter extends Filter {

  default boolean satisfy(long time, Object[] values) {
    // only use time to filter
    return satisfy(time, values[0]);
  }

  default boolean canSkip(Statistics<? extends Serializable> statistics) {
    return !satisfyStartEndTime(statistics.getStartTime(), statistics.getEndTime());
  }

  default boolean canSkip(IAlignedMetadataProvider alignedMetadata) {
    return canSkip(alignedMetadata.getTimeStatistics());
  }

  default boolean allSatisfy(Statistics<? extends Serializable> statistics) {
    return containStartEndTime(statistics.getStartTime(), statistics.getEndTime());
  }

  default boolean allSatisfy(IAlignedMetadataProvider alignedMetadata) {
    return allSatisfy(alignedMetadata.getTimeStatistics());
  }
}
