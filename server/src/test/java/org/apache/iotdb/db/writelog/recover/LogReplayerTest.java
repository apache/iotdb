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
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.storageGroup.StorageGroupProcessorException;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.junit.Test;

public class LogReplayerTest {

  @Test
  public void test() throws IOException, StorageGroupProcessorException, QueryProcessException {
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
    Schema schema = new Schema();

    try {
      for (int i = 0; i < 5; i++) {
        schema.registerMeasurement(
            new MeasurementSchema("sensor" + i, TSDataType.INT64, TSEncoding.PLAIN));
      }

      LogReplayer replayer = new LogReplayer(logNodePrefix, tsFile.getPath(), modFile,
          versionController, tsFileResource, schema, memTable, true);

      WriteLogNode node =
          MultiFileLogNodeManager.getInstance().getNode(logNodePrefix + tsFile.getName());
      node.write(new InsertPlan("device0", 100, "sensor0", String.valueOf(0)));
      node.write(new InsertPlan("device0", 2, "sensor1", String.valueOf(0)));
      for (int i = 1; i < 5; i++) {
        node.write(new InsertPlan("device" + i, i, "sensor" + i, String.valueOf(i)));
      }
      DeletePlan deletePlan = new DeletePlan(200, new Path("device0", "sensor0"));
      node.write(deletePlan);
      node.close();

      replayer.replayLogs();

      for (int i = 0; i < 5; i++) {
        ReadOnlyMemChunk memChunk = memTable.query("device" + i, "sensor" + i, TSDataType.INT64,
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
      assertEquals(new Deletion(new Path("device0", "sensor0"), 5, 200), mods[0]);

      assertEquals(2, (long) tsFileResource.getStartTimeMap().get("device0"));
      assertEquals(100, (long) tsFileResource.getEndTimeMap().get("device0"));
      for (int i = 1; i < 5; i++) {
        assertEquals(i, (long) tsFileResource.getStartTimeMap().get("device" + i));
        assertEquals(i, (long) tsFileResource.getEndTimeMap().get("device" + i));
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
