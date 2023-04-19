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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.read.SchemaRegionReadPlanFactory;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.write.SchemaRegionWritePlanFactory;
import org.apache.iotdb.db.metadata.query.info.ISchemaInfo;
import org.apache.iotdb.db.metadata.query.info.ITimeSeriesSchemaInfo;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.Comparator;
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
          SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
              new PartialPath("root.sg.d1.s1"),
              TSDataType.INT32,
              TSEncoding.PLAIN,
              CompressionType.UNCOMPRESSED,
              null,
              tags,
              null,
              null),
          -1);

      Template template = generateTemplate();
      schemaRegion.activateSchemaTemplate(
          SchemaRegionWritePlanFactory.getActivateTemplateInClusterPlan(
              new PartialPath("root.sg.d2"), 1, template.getId()),
          template);

      File snapshotDir = new File(config.getSchemaDir() + File.separator + "snapshot");
      snapshotDir.mkdir();
      schemaRegion.createSnapshot(snapshotDir);

      schemaRegion.loadSnapshot(snapshotDir);

      List<ITimeSeriesSchemaInfo> result =
          SchemaRegionTestUtil.showTimeseries(
              schemaRegion,
              SchemaRegionReadPlanFactory.getShowTimeSeriesPlan(
                  new PartialPath("root.sg.**"), false, "tag-key", "tag-value"));

      ITimeSeriesSchemaInfo seriesResult = result.get(0);
      Assert.assertEquals(
          new PartialPath("root.sg.d1.s1").getFullPath(), seriesResult.getFullPath());
      Map<String, String> resultTagMap = seriesResult.getTags();
      Assert.assertEquals(1, resultTagMap.size());
      Assert.assertEquals("tag-value", resultTagMap.get("tag-key"));

      simulateRestart();

      ISchemaRegion newSchemaRegion = getSchemaRegion("root.sg", 0);
      newSchemaRegion.loadSnapshot(snapshotDir);
      result =
          SchemaRegionTestUtil.showTimeseries(
              newSchemaRegion,
              SchemaRegionReadPlanFactory.getShowTimeSeriesPlan(
                  new PartialPath("root.sg.**"), false, "tag-key", "tag-value"));

      seriesResult = result.get(0);
      Assert.assertEquals(
          new PartialPath("root.sg.d1.s1").getFullPath(), seriesResult.getFullPath());
      resultTagMap = seriesResult.getTags();
      Assert.assertEquals(1, resultTagMap.size());
      Assert.assertEquals("tag-value", resultTagMap.get("tag-key"));

      result =
          SchemaRegionTestUtil.showTimeseries(
              newSchemaRegion,
              SchemaRegionReadPlanFactory.getShowTimeSeriesPlan(
                  new PartialPath("root.sg.*.s1"),
                  Collections.singletonMap(template.getId(), template)));
      result.sort(Comparator.comparing(ISchemaInfo::getFullPath));
      Assert.assertEquals(
          new PartialPath("root.sg.d1.s1").getFullPath(), result.get(0).getFullPath());
      Assert.assertEquals(
          new PartialPath("root.sg.d2.s1").getFullPath(), result.get(1).getFullPath());

    } finally {
      config.setSchemaRegionConsensusProtocolClass(schemaRegionConsensusProtocolClass);
    }
  }

  private Template generateTemplate() throws IllegalPathException {
    Template template =
        new Template(
            "t1",
            Collections.singletonList("s1"),
            Collections.singletonList(TSDataType.INT32),
            Collections.singletonList(TSEncoding.PLAIN),
            Collections.singletonList(CompressionType.GZIP));
    template.setId(1);
    return template;
  }

  @Test
  public void testEmptySnapshot() throws Exception {
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

      File snapshotDir = new File(config.getSchemaDir() + File.separator + "snapshot");
      snapshotDir.mkdir();
      schemaRegion.createSnapshot(snapshotDir);

      schemaRegion.loadSnapshot(snapshotDir);

      List<ITimeSeriesSchemaInfo> result =
          SchemaRegionTestUtil.showTimeseries(
              schemaRegion,
              SchemaRegionReadPlanFactory.getShowTimeSeriesPlan(
                  new PartialPath("root.sg.**"), false, "tag-key", "tag-value"));

      Assert.assertEquals(0, result.size());

      simulateRestart();

      ISchemaRegion newSchemaRegion = getSchemaRegion("root.sg", 0);
      newSchemaRegion.loadSnapshot(snapshotDir);
      result =
          SchemaRegionTestUtil.showTimeseries(
              newSchemaRegion,
              SchemaRegionReadPlanFactory.getShowTimeSeriesPlan(
                  new PartialPath("root.sg.**"), false, "tag-key", "tag-value"));

      Assert.assertEquals(0, result.size());
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
              SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
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
