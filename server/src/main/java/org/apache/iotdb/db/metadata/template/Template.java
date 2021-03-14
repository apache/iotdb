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
package org.apache.iotdb.db.metadata.template;

import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.db.qp.physical.crud.CreateTemplatePlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

public class Template {
  String name;
  Map<String, IMeasurementSchema> schemaMap = new HashMap<>();

  public Template(CreateTemplatePlan plan) {
    name = plan.getName();

    // put measurement into a map
    for (int i = 0; i < plan.getMeasurements().size(); i++) {
      IMeasurementSchema curSchema;
      // vector
      if (plan.getMeasurements().get(i).size() > 1) {
        curSchema =
            new VectorMeasurementSchema(
                (String[]) plan.getMeasurements().get(i).toArray(),
                (TSDataType[]) plan.getDataTypes().get(i).toArray(),
                (TSEncoding[]) plan.getEncodings().get(i).toArray(),
                plan.getCompressors().get(i));
      }
      // normal measurement
      else {
        curSchema =
            new MeasurementSchema(
                plan.getMeasurements().get(i).get(0),
                plan.getDataTypes().get(i).get(0),
                plan.getEncodings().get(i).get(0),
                plan.getCompressors().get(i));
      }

      for (String path : plan.getMeasurements().get(i)) {
        if (schemaMap.containsKey(path)) {
          throw new IllegalArgumentException(
              "Duplicate measurement name in create template plan. Name is :" + path);
        }

        schemaMap.put(path, curSchema);
      }
    }
  }
}
