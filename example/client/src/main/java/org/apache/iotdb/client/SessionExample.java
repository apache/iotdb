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

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.service.rpc.thrift.IoTDBDataType;
import org.apache.iotdb.session.IoTDBRowBatch;
import org.apache.iotdb.session.Session;

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
public class SessionExample {

  public static void main(String[] args) {
    Session session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    List<IoTDBDataType> dataTypes = new ArrayList<>();
    dataTypes.add(IoTDBDataType.FLOAT);
    dataTypes.add(IoTDBDataType.FLOAT);
    dataTypes.add(IoTDBDataType.FLOAT);

    IoTDBRowBatch rowBatch = new IoTDBRowBatch("root.sg1.d1", measurements, dataTypes);
    for (long i = 1; i <= 100; i++) {
      List<Object> values = new ArrayList<>();
      values.add(1.0f);
      values.add(1.0f);
      values.add(1.0f);
      rowBatch.addRow(i, values);
    }
    session.insertBatch(rowBatch);
    session.close();
  }

}
