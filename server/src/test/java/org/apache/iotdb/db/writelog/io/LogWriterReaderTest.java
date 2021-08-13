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

package org.apache.iotdb.db.writelog.io;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class LogWriterReaderTest {

  private static String filePath = "logtest.test";
  ByteBuffer logsBuffer = ByteBuffer.allocate(64 * 1024);
  List<PhysicalPlan> plans = new ArrayList<>();

  @Before
  public void prepare() throws IllegalPathException {
    if (new File(filePath).exists()) {
      new File(filePath).delete();
    }
    InsertRowPlan insertRowPlan1 =
        new InsertRowPlan(
            new PartialPath("d1"),
            10L,
            new String[] {"s1", "s2"},
            new TSDataType[] {TSDataType.INT64, TSDataType.INT64},
            new String[] {"1", "2"});
    InsertRowPlan insertRowPlan2 =
        new InsertRowPlan(
            new PartialPath("d1"),
            10L,
            new String[] {"s1", "s2"},
            new TSDataType[] {TSDataType.INT64, TSDataType.INT64},
            new String[] {"1", "2"});
    DeletePlan deletePlan = new DeletePlan(Long.MIN_VALUE, 10L, new PartialPath("root.d1.s1"));
    plans.add(insertRowPlan1);
    plans.add(insertRowPlan2);
    plans.add(deletePlan);
    for (PhysicalPlan plan : plans) {
      plan.serialize(logsBuffer);
    }
  }

  @Test
  public void testWriteAndRead() throws IOException {
    LogWriter writer =
        new LogWriter(
            filePath, IoTDBDescriptor.getInstance().getConfig().getForceWalPeriodInMs() == 0);
    writer.write(logsBuffer);
    try {
      writer.force();
      writer.close();
      SingleFileLogReader reader = new SingleFileLogReader(new File(filePath));
      List<PhysicalPlan> res = new ArrayList<>();
      while (reader.hasNext()) {
        res.add(reader.next());
      }
      for (int i = 0; i < plans.size(); i++) {
        assertEquals(plans.get(i), res.get(i));
      }
      reader.close();
    } finally {
      new File(filePath).delete();
    }
  }
}
