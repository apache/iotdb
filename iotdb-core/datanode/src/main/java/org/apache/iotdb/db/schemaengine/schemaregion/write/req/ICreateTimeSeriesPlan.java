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

package org.apache.iotdb.db.schemaengine.schemaregion.write.req;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegionPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanType;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanVisitor;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.util.Map;

public interface ICreateTimeSeriesPlan extends ISchemaRegionPlan {

  @Override
  default SchemaRegionPlanType getPlanType() {
    return SchemaRegionPlanType.CREATE_TIMESERIES;
  }

  @Override
  default <R, C> R accept(SchemaRegionPlanVisitor<R, C> visitor, C context) {
    return visitor.visitCreateTimeSeries(this, context);
  }

  MeasurementPath getPath();

  void setPath(MeasurementPath path);

  TSDataType getDataType();

  void setDataType(TSDataType dataType);

  CompressionType getCompressor();

  void setCompressor(CompressionType compressor);

  TSEncoding getEncoding();

  void setEncoding(TSEncoding encoding);

  Map<String, String> getAttributes();

  void setAttributes(Map<String, String> attributes);

  String getAlias();

  void setAlias(String alias);

  Map<String, String> getTags();

  void setTags(Map<String, String> tags);

  Map<String, String> getProps();

  void setProps(Map<String, String> props);

  long getTagOffset();

  void setTagOffset(long tagOffset);
}
