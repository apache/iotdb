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
package org.apache.iotdb.flink.sql.wrapper;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SchemaWrapper implements Serializable {
  private List<Tuple2<String, DataType>> schema;

  public SchemaWrapper(TableSchema schema) {
    this.schema = new ArrayList<>();

    for (String fieldName : schema.getFieldNames()) {
      if (fieldName == "Time_") {
        continue;
      }
      this.schema.add(new Tuple2<>(fieldName, schema.getFieldDataType(fieldName).get()));
    }
  }

  public List<Tuple2<String, DataType>> getSchema() {
    return schema;
  }
}
