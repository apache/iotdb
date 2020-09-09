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

package org.apache.iotdb.db.query.udf.example;

import java.util.Arrays;
import java.util.List;
import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowRecordIterationStrategy;
import org.apache.iotdb.db.query.udf.api.iterator.RowRecordIterator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;

public class Adder extends UDTF {

  private float addend;

  @Override
  public void initializeUDF(UDFParameters parameters, UDTFConfigurations configurations) {
    addend = parameters.getFloatOrDefault("addend", 0);
    configurations
        .setOutputDataType(TSDataType.FLOAT)
        .setRowRecordIterator("tablet", Arrays.asList(0, 1))
        .setRowRecordIterationStrategy("tablet", RowRecordIterationStrategy.FETCH_BY_ROW);
  }

  @Override
  public void transform() throws Exception {
    RowRecordIterator iterator = getRowRecordIterator("tablet");
    while (iterator.hasNextRowRecord()) {
      iterator.next();
      List<Field> fields = iterator.currentRowRecord().getFields();
      Field firstField = fields.get(0);
      Field secondField = fields.get(1);
      if (firstField.isNull() || secondField.isNull()) {
        continue;
      }
      collector.putFloat(iterator.currentTime(),
          firstField.getFloatV() + secondField.getFloatV() + addend);
    }
  }

  @Override
  public void finalizeUDF() {
    // do nothing
  }
}
