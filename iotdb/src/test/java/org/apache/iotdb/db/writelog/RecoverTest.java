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
package org.apache.iotdb.db.writelog;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.filenode.TsFileResource;
import org.apache.iotdb.db.engine.version.SysTimeVersionController;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.RecoverException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.UpdatePlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.writelog.node.ExclusiveWriteLogNode;
import org.apache.iotdb.db.writelog.recover.SeqTsFileRecoverPerformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RecoverTest {

  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private boolean enableWal;

  @Before
  public void setUp() throws Exception {
    enableWal = config.isEnableWal();
    config.setEnableWal(true);
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    config.setEnableWal(enableWal);
  }

  @Test
  public void testFullRecover() throws IOException, RecoverException, ProcessorException {
    // this test insert a log file and try to recover from these logs as if no previous attempts exist.
    File insertFile = new File("testTemp");
    FileSchema schema = new FileSchema();
    String deviceId = "root.testLogNode";
    schema.registerMeasurement(new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.PLAIN));
    schema.registerMeasurement(new MeasurementSchema("s2", TSDataType.INT32, TSEncoding.PLAIN));
    schema.registerMeasurement(new MeasurementSchema("s3", TSDataType.TEXT, TSEncoding.PLAIN));
    schema.registerMeasurement(new MeasurementSchema("s4", TSDataType.BOOLEAN, TSEncoding.PLAIN));

    TsFileIOWriter writer = new TsFileIOWriter(insertFile);
    writer.endFile(schema);
    TsFileResource tsFileResource = new TsFileResource(insertFile, true);
    try {
      MManager.getInstance().setStorageLevelToMTree(deviceId);
    } catch (PathErrorException ignored) {
    }
    ExclusiveWriteLogNode logNode = new ExclusiveWriteLogNode(deviceId);

    try {
      InsertPlan bwInsertPlan = new InsertPlan(1, deviceId, 100,
          new String[]{"s1", "s2", "s3", "s4"},
          new String[]{"1.0", "15", "str", "false"});
      UpdatePlan updatePlan = new UpdatePlan(0, 100, "2.0", new Path("root.logTestDevice.s1"));
      DeletePlan deletePlan = new DeletePlan(50, new Path("root.logTestDevice.s1"));

      List<PhysicalPlan> plansToCheck = new ArrayList<>();
      plansToCheck.add(bwInsertPlan);
      plansToCheck.add(updatePlan);
      plansToCheck.add(deletePlan);

      logNode.write(bwInsertPlan);
      logNode.write(updatePlan);
      logNode.notifyStartFlush();
      logNode.write(deletePlan);
      logNode.forceSync();

      SeqTsFileRecoverPerformer performer = new SeqTsFileRecoverPerformer(deviceId, schema,
          SysTimeVersionController.INSTANCE, tsFileResource);
      // used to check if logs are replayed in order
      performer.recover();

      // the log diretory should be empty now
      File logDir = new File(logNode.getLogDirectory());
      File[] files = logDir.listFiles();
      assertTrue(files == null || files.length == 0);
    } finally {
      logNode.delete();
      insertFile.delete();
    }
  }
}
