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

package org.apache.iotdb.udf;

import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;

/** This is an internal example of the UDTF implementation. */
public class UDTFExample implements UDTF {
  /*
   * SET STORAGE GROUP TO root.sg1;
   * CREATE TIMESERIES root.sg1.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN;
   * CREATE TIMESERIES root.sg1.d1.s2 WITH DATATYPE=INT32, ENCODING=PLAIN;
   * INSERT INTO root.sg1.d1(timestamp, s1, s2) VALUES (0, -1, 1);
   * INSERT INTO root.sg1.d1(timestamp, s1, s2) VALUES (1, -2, 2);
   * INSERT INTO root.sg1.d1(timestamp, s1, s2) VALUES (2, -3, 3);
   *
   * CREATE FUNCTION example AS "org.apache.iotdb.udf.UDTFExample";
   * SHOW FUNCTIONS;
   * SELECT s1, example(s1), s2, example(s2) FROM root.sg1.d1;
   */
  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(TSDataType.INT32);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws IOException {
    collector.putInt(row.getTime(), -row.getInt(0));
  }
}
