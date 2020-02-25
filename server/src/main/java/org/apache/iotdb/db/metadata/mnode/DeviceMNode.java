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
package org.apache.iotdb.db.metadata.mnode;

import java.util.Map;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class DeviceMNode extends InternalMNode {

  private static final long serialVersionUID = -1077855539671279042L;
  /**
   * Map for the schema in this device
   */
  private Map<String, MeasurementSchema> schemaMap;

  public DeviceMNode(MNode parent, String name,
      Map<String, MeasurementSchema> schemaMap) {
    super(parent, name);
    this.schemaMap = schemaMap;
  }

  public Map<String, MeasurementSchema> getSchemaMap() {
    return schemaMap;
  }

  public void addSchema(MNode child) {
    this.schemaMap.put(child.getName(), child.getSchema());
  }

  public void addMeasurementSchema(MeasurementSchema schema) {
    this.schemaMap.put(schema.getMeasurementId(), schema);
  }

}