/**
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
package org.apache.iotdb.client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.RowBatch;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

/**
 * you need to set storage group and create timeseries first from Client or JDBC
 *
 * for this example:
 *
 * SET STORAGE GROUP TO root.sg1
 * CREATE TIMESERIES root.sg1.d1.s1 WITH DATATYPE=FLOAT, ENCODING=RLE
 * CREATE TIMESERIES root.sg1.d1.s2 WITH DATATYPE=FLOAT, ENCODING=RLE
 * CREATE TIMESERIES root.sg1.d1.s3 WITH DATATYPE=FLOAT, ENCODING=RLE
 */
public class ClientExample {

  public static void main(String[] args) throws ClassNotFoundException {
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    Connection connection = null;
    try {
      connection = DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
      Statement statement = connection.createStatement();
      statement.execute("SET STORAGE GROUP TO root.sg1");
      statement.execute("CREATE TIMESERIES root.sg1.d1.s1 WITH DATATYPE=INT64, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg1.d1.s2 WITH DATATYPE=INT64, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg1.d1.s3 WITH DATATYPE=INT64, ENCODING=RLE");
    } catch (Exception e) {

    }

    Client session = new Client("127.0.0.1", 6667, "root", "root");
    session.open();

    Schema schema = new Schema();
    schema.registerMeasurement(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
    schema.registerMeasurement(new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
    schema.registerMeasurement(new MeasurementSchema("s3", TSDataType.INT64, TSEncoding.RLE));

    RowBatch rowBatch = schema.createRowBatch("root.sg1.d1", 100);

    long[] timestamps = rowBatch.timestamps;
    Object[] values = rowBatch.values;

    for (long time = 0; time < 30000; time++) {
      int row = rowBatch.batchSize++;
      timestamps[row] = time;
      for (int i = 0; i < 3; i++) {
        long[] sensor = (long[]) values[i];
        sensor[row] = time;
      }
      if (rowBatch.batchSize == rowBatch.getMaxBatchSize()) {
        session.insertBatch(rowBatch);
        rowBatch.reset();
      }
    }

    if (rowBatch.batchSize != 0) {
      session.insertBatch(rowBatch);
      rowBatch.reset();
    }

    session.close();
  }

}
