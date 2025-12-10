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

package org.apache.iotdb.db.pipe.sink.util.sorter;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.List;

/** Adapter for Tablet to implement InsertEventDataAdapter interface. */
public class TabletAdapter implements InsertEventDataAdapter {

  private final Tablet tablet;

  public TabletAdapter(final Tablet tablet) {
    this.tablet = tablet;
  }

  @Override
  public int getColumnCount() {
    final Object[] values = tablet.getValues();
    return values != null ? values.length : 0;
  }

  @Override
  public TSDataType getDataType(int columnIndex) {
    final List<IMeasurementSchema> schemas = tablet.getSchemas();
    if (schemas != null && columnIndex < schemas.size()) {
      final IMeasurementSchema schema = schemas.get(columnIndex);
      return schema != null ? schema.getType() : null;
    }
    return null;
  }

  @Override
  public BitMap[] getBitMaps() {
    return tablet.getBitMaps();
  }

  @Override
  public void setBitMaps(BitMap[] bitMaps) {
    tablet.setBitMaps(bitMaps);
  }

  @Override
  public Object[] getValues() {
    return tablet.getValues();
  }

  @Override
  public void setValue(int columnIndex, Object value) {
    tablet.getValues()[columnIndex] = value;
  }

  @Override
  public long[] getTimestamps() {
    return tablet.getTimestamps();
  }

  @Override
  public void setTimestamps(long[] timestamps) {
    tablet.setTimestamps(timestamps);
  }

  @Override
  public int getRowSize() {
    return tablet.getRowSize();
  }

  @Override
  public void setRowSize(int rowSize) {
    tablet.setRowSize(rowSize);
  }

  @Override
  public long getTimestamp(int rowIndex) {
    return tablet.getTimestamp(rowIndex);
  }

  @Override
  public IDeviceID getDeviceID(int rowIndex) {
    return tablet.getDeviceID(rowIndex);
  }

  @Override
  public boolean isDateStoredAsLocalDate(int columnIndex) {
    return true;
  }

  public Tablet getTablet() {
    return tablet;
  }
}
