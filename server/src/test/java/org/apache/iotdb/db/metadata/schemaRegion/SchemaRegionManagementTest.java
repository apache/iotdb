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

package org.apache.iotdb.db.metadata.schemaRegion;

import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaRegionManagementTest extends AbstractSchemaRegionTest {

  IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  public SchemaRegionManagementTest(SchemaRegionTestParams testParams) {
    super(testParams);
  }

  @Test
  public void testRatisModeSnapshot() throws Exception {
    String schemaRegionConsensusProtocolClass = config.getSchemaRegionConsensusProtocolClass();
    config.setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
    try {
      ISchemaRegion schemaRegion = getSchemaRegion("root.sg", 0);

      File mLogFile =
          SystemFileFactory.INSTANCE.getFile(
              schemaRegion.getStorageGroupFullPath()
                  + File.separator
                  + schemaRegion.getSchemaRegionId().getId(),
              MetadataConstant.METADATA_LOG);
      Assert.assertFalse(mLogFile.exists());

      Map<String, String> tags = new HashMap<>();
      tags.put("tag-key", "tag-value");
      schemaRegion.createTimeseries(
          new CreateTimeSeriesPlan(
              new PartialPath("root.sg.d1.s1"),
              TSDataType.INT32,
              TSEncoding.PLAIN,
              CompressionType.UNCOMPRESSED,
              null,
              tags,
              null,
              null),
          -1);

      File snapshotDir = new File(config.getSchemaDir() + File.separator + "snapshot");
      snapshotDir.mkdir();
      schemaRegion.createSnapshot(snapshotDir);

      schemaRegion.loadSnapshot(snapshotDir);

      Pair<List<ShowTimeSeriesResult>, Integer> result =
          schemaRegion.showTimeseries(
              new ShowTimeSeriesPlan(
                  new PartialPath("root.sg.**"), false, "tag-key", "tag-value", 0, 0, false),
              null);

      ShowTimeSeriesResult seriesResult = result.left.get(0);
      Assert.assertEquals(new PartialPath("root.sg.d1.s1").getFullPath(), seriesResult.getName());
      Map<String, String> resultTagMap = seriesResult.getTag();
      Assert.assertEquals(1, resultTagMap.size());
      Assert.assertEquals("tag-value", resultTagMap.get("tag-key"));

      simulateRestart();

      ISchemaRegion newSchemaRegion = getSchemaRegion("root.sg", 0);
      newSchemaRegion.loadSnapshot(snapshotDir);
      result =
          newSchemaRegion.showTimeseries(
              new ShowTimeSeriesPlan(
                  new PartialPath("root.sg.**"), false, "tag-key", "tag-value", 0, 0, false),
              null);

      seriesResult = result.left.get(0);
      Assert.assertEquals(new PartialPath("root.sg.d1.s1").getFullPath(), seriesResult.getName());
      resultTagMap = seriesResult.getTag();
      Assert.assertEquals(1, resultTagMap.size());
      Assert.assertEquals("tag-value", resultTagMap.get("tag-key"));

    } finally {
      config.setSchemaRegionConsensusProtocolClass(schemaRegionConsensusProtocolClass);
    }
  }

  @Test
  @Ignore
  public void testSnapshotPerformance() throws Exception {
    String schemaRegionConsensusProtocolClass = config.getSchemaRegionConsensusProtocolClass();
    config.setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
    try {
      ISchemaRegion schemaRegion = getSchemaRegion("root.sg", 0);

      Map<String, String> tags = new HashMap<>();
      tags.put("tag-key", "tag-value");

      long time = System.currentTimeMillis();
      for (int i = 0; i < 1000; i++) {
        for (int j = 0; j < 1000; j++) {
          schemaRegion.createTimeseries(
              new CreateTimeSeriesPlan(
                  new PartialPath("root.sg.d" + i + ".s" + j),
                  TSDataType.INT32,
                  TSEncoding.PLAIN,
                  CompressionType.UNCOMPRESSED,
                  null,
                  tags,
                  null,
                  null),
              -1);
        }
      }
      System.out.println(
          "Timeseries creation costs " + (System.currentTimeMillis() - time) + "ms.");

      File snapshotDir = new File(config.getSchemaDir() + File.separator + "snapshot");
      snapshotDir.mkdir();
      schemaRegion.createSnapshot(snapshotDir);

      schemaRegion.loadSnapshot(snapshotDir);
    } finally {
      config.setSchemaRegionConsensusProtocolClass(schemaRegionConsensusProtocolClass);
    }
  }
}
