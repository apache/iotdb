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

package org.apache.iotdb.db.pipe.event.common.row;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;

/**
 * The pipe framework will reset a new {@link Tablet} when this kind of {@link PipeRow} is
 * encountered.
 */
public class PipeResetTabletRow extends PipeRow {

  public PipeResetTabletRow(
      int rowIndex,
      String deviceId,
      boolean isAligned,
      MeasurementSchema[] measurementSchemaList,
      long[] timestampColumn,
      TSDataType[] valueColumnTypes,
      Object[] valueColumns,
      BitMap[] bitMaps,
      String[] columnNameStringList) {
    super(
        rowIndex,
        deviceId,
        isAligned,
        measurementSchemaList,
        timestampColumn,
        valueColumnTypes,
        valueColumns,
        bitMaps,
        columnNameStringList);
  }
}
