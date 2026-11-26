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

/**
 * Adapter interface to encapsulate common operations needed for sorting and deduplication. This
 * interface allows the sorter to work with both Tablet and InsertTabletStatement.
 */
public interface InsertEventDataAdapter {

  /**
   * Get the number of columns.
   *
   * @return number of columns
   */
  int getColumnCount();

  /**
   * Get data type for a specific column.
   *
   * @param columnIndex column index
   * @return data type of the column
   */
  TSDataType getDataType(int columnIndex);

  /**
   * Get bit maps for null values.
   *
   * @return array of bit maps, may be null
   */
  BitMap[] getBitMaps();

  /**
   * Set bit maps for null values.
   *
   * @param bitMaps array of bit maps
   */
  void setBitMaps(BitMap[] bitMaps);

  /**
   * Get value arrays for all columns.
   *
   * @return array of value arrays (Object[])
   */
  Object[] getValues();

  /**
   * Set value array for a specific column.
   *
   * @param columnIndex column index
   * @param value value array
   */
  void setValue(int columnIndex, Object value);

  /**
   * Get timestamps array.
   *
   * @return array of timestamps
   */
  long[] getTimestamps();

  /**
   * Set timestamps array.
   *
   * @param timestamps array of timestamps
   */
  void setTimestamps(long[] timestamps);

  /**
   * Get row size/count.
   *
   * @return number of rows
   */
  int getRowSize();

  /**
   * Set row size/count.
   *
   * @param rowSize number of rows
   */
  void setRowSize(int rowSize);

  /**
   * Get timestamp at a specific row index.
   *
   * @param rowIndex row index
   * @return timestamp value
   */
  long getTimestamp(int rowIndex);

  /**
   * Get device ID at a specific row index (for table model).
   *
   * @param rowIndex row index
   * @return device ID
   */
  IDeviceID getDeviceID(int rowIndex);

  /**
   * Check if the DATE type column value is stored as LocalDate[] (Tablet) or int[] (Statement).
   *
   * @param columnIndex column index
   * @return true if DATE type is stored as LocalDate[], false if stored as int[]
   */
  boolean isDateStoredAsLocalDate(int columnIndex);
}
