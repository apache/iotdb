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

package org.apache.iotdb.db.metadata.plan.schemaregion.write;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.plan.schemaregion.ISchemaRegionPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.SchemaRegionPlanType;
import org.apache.iotdb.db.metadata.plan.schemaregion.SchemaRegionPlanVisitor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.List;
import java.util.Map;

public interface ICreateAlignedTimeSeriesPlan extends ISchemaRegionPlan {

  @Override
  default SchemaRegionPlanType getPlanType() {
    return SchemaRegionPlanType.CREATE_ALIGNED_TIMESERIES;
  }

  @Override
  default <R, C> R accept(SchemaRegionPlanVisitor<R, C> visitor, C context) {
    return visitor.visitCreateAlignedTimeSeries(this, context);
  }

  PartialPath getDevicePath();

  void setDevicePath(PartialPath devicePath);

  List<String> getMeasurements();

  void setMeasurements(List<String> measurements);

  List<TSDataType> getDataTypes();

  void setDataTypes(List<TSDataType> dataTypes);

  List<TSEncoding> getEncodings();

  void setEncodings(List<TSEncoding> encodings);

  List<CompressionType> getCompressors();

  void setCompressors(List<CompressionType> compressors);

  List<String> getAliasList();

  void setAliasList(List<String> aliasList);

  List<Map<String, String>> getTagsList();

  void setTagsList(List<Map<String, String>> tagsList);

  List<Map<String, String>> getAttributesList();

  void setAttributesList(List<Map<String, String>> attributesList);

  List<Long> getTagOffsets();

  void setTagOffsets(List<Long> tagOffsets);
}
