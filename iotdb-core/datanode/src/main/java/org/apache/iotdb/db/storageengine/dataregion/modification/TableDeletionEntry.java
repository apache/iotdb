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
package org.apache.iotdb.db.storageengine.dataregion.modification;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.utils.ModificationUtils;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.TimeRange;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

public class TableDeletionEntry extends ModEntry {
  private DeletionPredicate predicate;

  public TableDeletionEntry() {
    super(ModType.TABLE_DELETION);
  }

  public TableDeletionEntry(DeletionPredicate predicate, TimeRange timeRange) {
    this();
    this.predicate = predicate;
    this.timeRange = timeRange;
  }

  @Override
  public long serialize(OutputStream stream) throws IOException {
    long size = super.serialize(stream);
    size += predicate.serialize(stream);
    return size;
  }

  @Override
  public long serialize(ByteBuffer buffer) {
    long size = super.serialize(buffer);
    size += predicate.serialize(buffer);
    return size;
  }

  @Override
  public void deserialize(InputStream stream) throws IOException {
    super.deserialize(stream);
    predicate = new DeletionPredicate();
    predicate.deserialize(stream);
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    super.deserialize(buffer);
    predicate = new DeletionPredicate();
    predicate.deserialize(buffer);
  }

  @Override
  public boolean matches(PartialPath path) {
    IDeviceID iDeviceID = path.getIDeviceID();
    return predicate.matches(iDeviceID);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + predicate.serializedSize();
  }

  @Override
  public boolean affects(IDeviceID deviceID, long startTime, long endTime) {
    return predicate.matches(deviceID)
        && ModificationUtils.overlap(getStartTime(), getEndTime(), startTime, endTime);
  }

  @Override
  public boolean affects(IDeviceID deviceID) {
    return predicate.matches(deviceID);
  }

  @Override
  public boolean affects(String measurementID) {
    return predicate.affects(measurementID);
  }

  @Override
  public boolean affectsAll(IDeviceID deviceID) {
    return predicate.matches(deviceID) && predicate.getMeasurementNames().isEmpty();
  }

  @Override
  public PartialPath keyOfPatternTree() {
    return new PartialPath(
        new String[] {predicate.getTableName(), IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD});
  }

  @Override
  public String toString() {
    return "TableDeletionEntry{" + "predicate=" + predicate + ", timeRange=" + timeRange + '}';
  }

  @Override
  public int compareTo(ModEntry o) {
    if (this.getType() != o.getType()) {
      return Byte.compare(this.getType().getTypeNum(), o.getType().getTypeNum());
    }
    return 0;
  }

  public TableDeletionEntry clone() {
    TimeRange timeRangeCopy = new TimeRange(timeRange.getMin(), timeRange.getMax());
    return new TableDeletionEntry(predicate, timeRangeCopy);
  }

  public String getTableName() {
    return predicate.getTableName();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableDeletionEntry that = (TableDeletionEntry) o;
    return Objects.equals(predicate, that.predicate) && Objects.equals(timeRange, that.timeRange);
  }

  @Override
  public int hashCode() {
    return Objects.hash(predicate, timeRange);
  }

  public DeletionPredicate getPredicate() {
    return predicate;
  }
}
