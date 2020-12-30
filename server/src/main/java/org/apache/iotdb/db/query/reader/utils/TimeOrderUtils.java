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

import java.util.Comparator;
import java.util.List;
import java.util.function.ToLongFunction;

public interface TimeOrderUtils {

  long getOrderTime(Statistics<? extends Object> statistics);

  /**
   * get order time for series in tsfile
   * @param fileResource the tsfile
   * @param partialPath the series path
   * @return
   */
  long getOrderTime(TsFileResource fileResource, PartialPath partialPath);

  long getOverlapCheckTime(Statistics<? extends Object> range);

  boolean isOverlapped(Statistics<? extends Object> left, Statistics<? extends Object> right);

  boolean isOverlapped(long time, Statistics<? extends Object> right);

  /**
   * determine whether the tsfile contains the series data who's timestamp is time
   * @param time the time
   * @param right tsfile
   * @param partialPath  series path
   * @return
   */
  boolean isOverlapped(long time, TsFileResource right, PartialPath partialPath);

  TsFileResource getNextSeqFileResource(List<TsFileResource> seqResources, boolean isDelete);

  <T> Comparator<T> comparingLong(ToLongFunction<? super T> keyExtractor);

  long getCurrentEndPoint(long time, Statistics<? extends Object> statistics);

  long getCurrentEndPoint(Statistics<? extends Object> seqStatistics,
                          Statistics<? extends Object> unseqStatistics);

  boolean isExcessEndpoint(long time, long endpointTime);

  /**
   * Return true if taking first page reader from seq readers
   */
  boolean isTakeSeqAsFirst(Statistics<? extends Object> seqStatistics,
                           Statistics<? extends Object> unseqStatistics);

  boolean getAscending();
}
