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
package org.apache.iotdb.db.metadata.tagSchemaRegion.utils;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.idtable.entry.DiskSchemaEntry;
import org.apache.iotdb.db.metadata.idtable.entry.SchemaEntry;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

/** process MeasurementPath */
public class MeasurementPathUtils {

  /**
   * generate MeasurementPath
   *
   * @param devicePath device path
   * @param measurement measurement
   * @param schemaEntry schema entry
   * @param isAligned is aligned
   * @return MeasurementPath
   * @throws IllegalPathException
   */
  public static MeasurementPath generateMeasurementPath(
      String devicePath, String measurement, SchemaEntry schemaEntry, boolean isAligned)
      throws IllegalPathException {
    MeasurementPath measurementPath =
        new MeasurementPath(
            devicePath,
            measurement,
            new MeasurementSchema(
                measurement,
                schemaEntry.getTSDataType(),
                schemaEntry.getTSEncoding(),
                schemaEntry.getCompressionType()));
    measurementPath.setUnderAlignedEntity(isAligned);
    return measurementPath;
  }

  /**
   * generate MeasurementPath
   *
   * @param diskSchemaEntry disk schema entry
   * @return MeasurementPath
   * @throws IllegalPathException
   */
  public static MeasurementPath generateMeasurementPath(DiskSchemaEntry diskSchemaEntry)
      throws IllegalPathException {
    MeasurementPath measurementPath =
        new MeasurementPath(
            new PartialPath(diskSchemaEntry.seriesKey),
            new MeasurementSchema(
                diskSchemaEntry.measurementName,
                TSDataType.deserialize(diskSchemaEntry.type),
                TSEncoding.deserialize(diskSchemaEntry.encoding),
                CompressionType.deserialize(diskSchemaEntry.compressor)));
    measurementPath.setUnderAlignedEntity(diskSchemaEntry.isAligned);
    return measurementPath;
  }
}
