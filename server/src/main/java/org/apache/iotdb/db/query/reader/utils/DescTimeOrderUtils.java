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

package org.apache.iotdb.db.query.reader.utils;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.ToLongFunction;

public class DescTimeOrderUtils implements TimeOrderUtils {

  @Override
  public long getOrderTime(Statistics statistics) {
    return statistics.getEndTime();
  }

  @Override
  public long getOrderTime(TsFileResource fileResource, PartialPath partialPath) {
    return fileResource.getEndTime(partialPath.getDevice());
  }

  @Override
  public long getOverlapCheckTime(Statistics range) {
    return range.getStartTime();
  }

  @Override
  public boolean isOverlapped(Statistics left, Statistics right) {
    return left.getStartTime() <= right.getEndTime();
  }

  @Override
  public boolean isOverlapped(long time, Statistics right) {
    return time <= right.getEndTime();
  }

  @Override
  public boolean isOverlapped(long time, TsFileResource right, PartialPath partialPath) {
    return time <= right.getEndTime(partialPath.getDevice());
  }

  @Override
  public TsFileResource getNextSeqFileResource(List<TsFileResource> seqResources,
                                               boolean isDelete) {
    if (isDelete) {
      return seqResources.remove(seqResources.size() - 1);
    }
    return seqResources.get(seqResources.size() - 1);
  }

  @Override
  public <T> Comparator<T> comparingLong(ToLongFunction<? super T> keyExtractor) {
    Objects.requireNonNull(keyExtractor);
    return (Comparator<T> & Serializable)
      (c1, c2) -> Long.compare(keyExtractor.applyAsLong(c2), keyExtractor.applyAsLong(c1));
  }

  @Override
  public long getCurrentEndPoint(long time, Statistics<? extends Object> statistics) {
    return Math.max(time, statistics.getStartTime());
  }

  @Override
  public long getCurrentEndPoint(Statistics<? extends Object> seqStatistics,
                                 Statistics<? extends Object> unseqStatistics) {
    return Math.max(seqStatistics.getStartTime(), unseqStatistics.getStartTime());
  }

  @Override
  public boolean isExcessEndpoint(long time, long endpointTime) {
    return time < endpointTime;
  }

  @Override
  public boolean isTakeSeqAsFirst(Statistics<? extends Object> seqStatistics,
                                  Statistics<? extends Object> unseqStatistics) {
    return seqStatistics.getEndTime() > unseqStatistics.getEndTime();
  }

  @Override
  public boolean getAscending() {
    return false;
  }
}
