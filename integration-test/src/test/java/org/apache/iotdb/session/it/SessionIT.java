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
package org.apache.iotdb.session.it;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class SessionIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testInsertByStrAndSelectFailedData() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      String deviceId = "root.sg1.d1";

      session.createTimeseries(
          deviceId + ".s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.UNCOMPRESSED);
      session.createTimeseries(
          deviceId + ".s2", TSDataType.INT64, TSEncoding.RLE, CompressionType.UNCOMPRESSED);
      session.createTimeseries(
          deviceId + ".s3", TSDataType.INT64, TSEncoding.RLE, CompressionType.UNCOMPRESSED);
      session.createTimeseries(
          deviceId + ".s4", TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.UNCOMPRESSED);

      List<MeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
      schemaList.add(new MeasurementSchema("s2", TSDataType.DOUBLE, TSEncoding.RLE));
      schemaList.add(new MeasurementSchema("s3", TSDataType.TEXT, TSEncoding.PLAIN));
      schemaList.add(new MeasurementSchema("s4", TSDataType.INT64, TSEncoding.PLAIN));

      Tablet tablet = new Tablet("root.sg1.d1", schemaList, 10);

      long[] timestamps = tablet.timestamps;
      Object[] values = tablet.values;

      for (long time = 0; time < 10; time++) {
        int row = tablet.rowSize++;
        timestamps[row] = time;
        long[] sensor = (long[]) values[0];
        sensor[row] = time;
        double[] sensor2 = (double[]) values[1];
        sensor2[row] = 0.1 + time;
        Binary[] sensor3 = (Binary[]) values[2];
        sensor3[row] = Binary.valueOf("ha" + time);
        long[] sensor4 = (long[]) values[3];
        sensor4[row] = time;
      }

      try {
        session.insertTablet(tablet);
        fail();
      } catch (StatementExecutionException e) {
        // ignore
      }

      SessionDataSet dataSet =
          session.executeQueryStatement("select s1, s2, s3, s4 from root.sg1.d1");
      int i = 0;
      while (dataSet.hasNext()) {
        RowRecord record = dataSet.next();
        Assert.assertEquals(i, record.getFields().get(0).getLongV());
        Assert.assertNull(record.getFields().get(1).getDataType());
        Assert.assertNull(record.getFields().get(2).getDataType());
        Assert.assertEquals(i, record.getFields().get(3).getDoubleV(), 0.00001);
        i++;
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
