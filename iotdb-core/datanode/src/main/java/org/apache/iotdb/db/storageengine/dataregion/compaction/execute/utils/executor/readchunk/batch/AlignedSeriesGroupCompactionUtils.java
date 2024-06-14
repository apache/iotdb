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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.batch;

import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class AlignedSeriesGroupCompactionUtils {

  private AlignedSeriesGroupCompactionUtils() {}

  public static List<IMeasurementSchema> selectColumnGroupToCompact(
      List<IMeasurementSchema> schemaList, Set<String> compactedMeasurements) {
    int compactColumnNum =
        IoTDBDescriptor.getInstance().getConfig().getCompactionMaxAlignedSeriesNumInOneBatch();
    List<IMeasurementSchema> selectedColumnGroup = new ArrayList<>(compactColumnNum);
    for (IMeasurementSchema schema : schemaList) {
      if (!schema.getType().equals(TSDataType.TEXT)) {
        continue;
      }
      if (compactedMeasurements.contains(schema.getMeasurementId())) {
        continue;
      }
      compactedMeasurements.add(schema.getMeasurementId());
      selectedColumnGroup.add(schema);
      if (compactedMeasurements.size() == schemaList.size()) {
        return selectedColumnGroup;
      }
    }
    for (IMeasurementSchema schema : schemaList) {
      if (compactedMeasurements.contains(schema.getMeasurementId())) {
        continue;
      }
      selectedColumnGroup.add(schema);
      compactedMeasurements.add(schema.getMeasurementId());
      if (selectedColumnGroup.size() == compactColumnNum) {
        break;
      }
      if (compactedMeasurements.size() == schemaList.size()) {
        break;
      }
    }
    return selectedColumnGroup;
  }
}
