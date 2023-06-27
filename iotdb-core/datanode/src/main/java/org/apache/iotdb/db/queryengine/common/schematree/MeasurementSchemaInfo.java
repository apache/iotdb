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

package org.apache.iotdb.db.queryengine.common.schematree;

import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

/**
 * This class acts as common measurement schema format during system module interactions, mainly in
 * analyzer and SchemaFetcher. Currently, this class cooperates with DeviceSchemaInfo and wraps
 * measurement name, alias and MeasurementSchema, which are necessary to construct schemaTree for
 * Query and Insertion.
 */
public class MeasurementSchemaInfo implements IMeasurementSchemaInfo {

  private final String name;
  private final String alias;
  private final IMeasurementSchema schema;

  public MeasurementSchemaInfo(String name, IMeasurementSchema schema, String alias) {
    this.name = name;
    this.schema = schema;
    this.alias = alias;
  }

  public String getName() {
    return name;
  }

  public IMeasurementSchema getSchema() {
    return schema;
  }

  public MeasurementSchema getSchemaAsMeasurementSchema() {
    if (this.isLogicalView()) {
      return null;
    } else {
      return (MeasurementSchema) this.schema;
    }
  }

  @Override
  public LogicalViewSchema getSchemaAsLogicalViewSchema() {
    if (this.isLogicalView()) {
      return (LogicalViewSchema) this.schema;
    }
    return null;
  }

  public String getAlias() {
    return alias;
  }

  @Override
  public boolean isLogicalView() {
    return this.schema.isLogicalView();
  }
}
