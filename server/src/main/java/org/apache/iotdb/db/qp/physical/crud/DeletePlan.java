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
package org.apache.iotdb.db.qp.physical.crud;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;

public class DeletePlan extends PhysicalPlan {

  private Filter timeFilter;
  private long minTime = Long.MIN_VALUE;
  private long maxTime = Long.MAX_VALUE;
  private List<Path> paths = new ArrayList<>();

  public DeletePlan() {
    super(false, Operator.OperatorType.DELETE);
  }

  /**
   * constructor of DeletePlan with single path.
   *
   * @param timeFilter delete time (data range to be deleted in the timeseries whose time is <=
   *                   deleteTime and time is >= deleteTime</=>)
   * @param path       time series path
   */
  public DeletePlan(Filter timeFilter, Path path) {
    super(false, Operator.OperatorType.DELETE);
    this.timeFilter = timeFilter;
    this.paths.add(path);
  }

  /**
   * constructor of DeletePlan with multiple paths.
   *
   * @param deleteTime delete time (data points to be deleted in the timeseries whose time is <=
   *                   deleteTime)
   * @param paths      time series paths in List structure
   */
  public DeletePlan(Filter deleteTime, List<Path> paths) {
    super(false, Operator.OperatorType.DELETE);
    this.timeFilter = deleteTime;
    this.paths = paths;
  }

  public Filter getTimeFilter() {
    return timeFilter;
  }

  public void setTimeFilter(Filter timeFilter) {
    this.timeFilter = timeFilter;
  }

  public void addPath(Path path) {
    this.paths.add(path);
  }

  public void addPaths(List<Path> paths) {
    this.paths.addAll(paths);
  }

  @Override
  public List<Path> getPaths() {
    return paths;
  }

  public void setPaths(List<Path> paths) {
    this.paths = paths;
  }

  @Override
  public int hashCode() {
    return Objects.hash(timeFilter, paths);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DeletePlan that = (DeletePlan) o;
    return timeFilter == that.timeFilter && Objects.equals(paths, that.paths);
  }

  @Override
  public void serializeTo(DataOutputStream stream) throws IOException {
    int type = PhysicalPlanType.DELETE.ordinal();
    stream.writeByte((byte) type);
    timeFilter.serialize(stream);
    putString(stream, paths.get(0).getFullPath());
  }

  @Override
  public void serializeTo(ByteBuffer buffer) {
    int type = PhysicalPlanType.DELETE.ordinal();
    buffer.put((byte) type);
    timeFilter.serialize(buffer);
    putString(buffer, paths.get(0).getFullPath());
  }

  @Override
  public void deserializeFrom(ByteBuffer buffer) {
    this.timeFilter = FilterFactory.deserialize(buffer);
    this.paths = new ArrayList();
    this.paths.add(new Path(readString(buffer)));
  }

  public long getMinTime() {
    return minTime;
  }

  public void setMinTime(long minTime) {
    this.minTime = minTime;
  }

  public long getMaxTime() {
    return maxTime;
  }

  public void setMaxTime(long maxTime) {
    this.maxTime = maxTime;
  }
}
