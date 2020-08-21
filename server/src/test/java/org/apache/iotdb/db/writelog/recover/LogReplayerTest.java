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

package org.apache.iotdb.db.writelog.recover;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LogReplayerTest {

  @Before
  public void before() {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void after() throws IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void test()
      throws IOException, StorageGroupProcessorException, QueryProcessException, MetadataException {
    String logNodePrefix = "testLogNode";
    File tsFile = SystemFileFactory.INSTANCE.getFile("temp", "1-1-1.tsfile");
    File modF = SystemFileFactory.INSTANCE.getFile("test.mod");
    ModificationFile modFile = new ModificationFile(modF.getPath());
    VersionController versionController = new VersionController() {
      @Override
      public long nextVersion() {
        return 5;
      }

      @Override
      public long currVersion() {
        return 5;
      }
    };
    TsFileResource tsFileResource = new TsFileResource(tsFile);
    IMemTable memTable = new PrimitiveMemTable();

    IoTDB.metaManager.setStorageGroup(new PartialPath("root.sg"));
    try {
      for (int i = 0; i < 5; i++) {
        for (int j = 0; j < 5; j++) {
          IoTDB.metaManager
              .createTimeseries(new PartialPath("root.sg.device" + i + ".sensor" + j), TSDataType.INT64,
                  TSEncoding.PLAIN, TSFileDescriptor.getInstance().getConfig().getCompressor(),
                  Collections.emptyMap());
        }
      }

      LogReplayer replayer = new LogReplayer(logNodePrefix, tsFile.getPath(), modFile,
          versionController, tsFileResource, memTable, false);

      WriteLogNode node =
          MultiFileLogNodeManager.getInstance().getNode(logNodePrefix + tsFile.getName());
      node.write(
          new InsertRowPlan(new PartialPath("root.sg.device0"), 100, "sensor0", TSDataType.INT64,
              String.valueOf(0)));
      node.write(
          new InsertRowPlan(new PartialPath("root.sg.device0"), 2, "sensor1", TSDataType.INT64, String.valueOf(0)));
      for (int i = 1; i < 5; i++) {
        node.write(new InsertRowPlan(new PartialPath("root.sg.device" + i), i, "sensor" + i, TSDataType.INT64,
            String.valueOf(i)));
      }
      DeletePlan deletePlan = new DeletePlan(0, 200, new PartialPath("root.sg.device0.sensor0"));
      node.write(deletePlan);
      node.close();

      replayer.replayLogs();

      for (int i = 0; i < 5; i++) {
        ReadOnlyMemChunk memChunk = memTable
            .query("root.sg.device" + i, "sensor" + i, TSDataType.INT64,
                TSEncoding.RLE, Collections.emptyMap(), Long.MIN_VALUE);
        IPointReader iterator = memChunk.getPointReader();
        if (i == 0) {
          assertFalse(iterator.hasNextTimeValuePair());
        } else {
          assertTrue(iterator.hasNextTimeValuePair());
          TimeValuePair timeValuePair = iterator.nextTimeValuePair();
          assertEquals(i, timeValuePair.getTimestamp());
          assertEquals(i, timeValuePair.getValue().getLong());
          assertFalse(iterator.hasNextTimeValuePair());
        }
      }

      Modification[] mods = modFile.getModifications().toArray(new Modification[0]);
      assertEquals(1, mods.length);
      assertEquals("root.sg.device0.sensor0", mods[0].getPathString());
      assertEquals(5, mods[0].getVersionNum());
      assertEquals(((Deletion) mods[0]).getEndTime(), 200);

      assertEquals(2, tsFileResource.getStartTime("root.sg.device0"));
      assertEquals(100, tsFileResource.getEndTime("root.sg.device0"));
      for (int i = 1; i < 5; i++) {
        assertEquals(i, tsFileResource.getStartTime("root.sg.device" + i));
        assertEquals(i, tsFileResource.getEndTime("root.sg.device" + i));
      }
    } finally {
      modFile.close();
      MultiFileLogNodeManager.getInstance().deleteNode(logNodePrefix + tsFile.getName());
      modF.delete();
      tsFile.delete();
      tsFile.getParentFile().delete();
    }
  }
}
