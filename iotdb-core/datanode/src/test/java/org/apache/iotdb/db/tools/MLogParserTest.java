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

package org.apache.iotdb.db.tools;

import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.SchemaRegionWritePlanFactory;
import org.apache.iotdb.db.tools.schema.MLogParser;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MLogParserTest {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private String[] storageGroups = new String[] {"root.sg0", "root.sg1", "root.sg"};
  private int[] schemaRegionIds = new int[] {0, 1, 2};

  /*
   * For root.sg0, we prepare 50 CreateTimeseriesPlan.
   * For root.sg1, we prepare 50 CreateTimeseriesPlan, 1 DeleteTimeseriesPlan, 1 ChangeTagOffsetPlan and 1 ChangeAliasPlan.
   * For root.sg, we prepare none schema plan.
   *
   * For root.ln.cc, we create it and then delete it, thus there's no mlog of root.ln.cc.
   * There' still 1 CreateTemplatePlan in template_log.bin
   *
   * */
  private int[] mlogLineNum = new int[] {50, 54, 0};

  private String schemaRegionConsensusProtocolClass;

  @Before
  public void setUp() {
    schemaRegionConsensusProtocolClass = config.getSchemaRegionConsensusProtocolClass();
    config.setSchemaRegionConsensusProtocolClass(ConsensusFactory.SIMPLE_CONSENSUS);
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    File file = new File("target" + File.separator + "tmp" + File.separator + "text.mlog");
    file.delete();
    file = new File("target" + File.separator + "tmp" + File.separator + "text.snapshot");
    file.delete();
    EnvironmentUtils.cleanEnv();
    config.setSchemaRegionConsensusProtocolClass(schemaRegionConsensusProtocolClass);
  }

  private void prepareData() throws Exception {
    // prepare data
    SchemaEngine schemaEngine = SchemaEngine.getInstance();
    for (int i = 0; i < storageGroups.length; i++) {
      SchemaEngine.getInstance()
          .createSchemaRegion(
              new PartialPath(storageGroups[i]), new SchemaRegionId(schemaRegionIds[i]));
    }

    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 5; j++) {
        for (int k = 0; k < 10; k++) {
          try {
            schemaEngine
                .getSchemaRegion(new SchemaRegionId(schemaRegionIds[i]))
                .createTimeSeries(
                    SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
                        new MeasurementPath("root.sg" + i + "." + "device" + j + "." + "s" + k),
                        TSDataType.INT32,
                        TSEncoding.PLAIN,
                        CompressionType.GZIP,
                        null,
                        null,
                        null,
                        null),
                    -1);
          } catch (MetadataException e) {
            e.printStackTrace();
          }
        }
      }
    }

    try {
      PathPatternTree patternTree = new PathPatternTree();
      patternTree.appendPathPattern(new PartialPath("root.sg1.device1.s1"));
      patternTree.constructTree();
      schemaEngine.getSchemaRegion(new SchemaRegionId(1)).constructSchemaBlackList(patternTree);
      schemaEngine.getSchemaRegion(new SchemaRegionId(1)).deleteTimeseriesInBlackList(patternTree);
      Map<String, String> tags = new HashMap<String, String>();
      tags.put("tag1", "value1");
      schemaEngine
          .getSchemaRegion(new SchemaRegionId(1))
          .addTags(tags, new PartialPath("root.sg1.device1.s2"));
      schemaEngine
          .getSchemaRegion(new SchemaRegionId(1))
          .upsertAliasAndTagsAndAttributes(
              "hello", null, null, new PartialPath("root.sg1.device1.s3"));
    } catch (MetadataException | IOException e) {
      e.printStackTrace();
    }

    try {
      SchemaEngine.getInstance()
          .createSchemaRegion(new PartialPath("root.sg"), new SchemaRegionId(schemaRegionIds[2]));
    } catch (MetadataException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testMLogParser() throws Exception {
    prepareData();

    SchemaEngine.getInstance().forceMlog();

    for (int i = 0; i < storageGroups.length; i++) {
      testParseMLog(storageGroups[i], schemaRegionIds[i], mlogLineNum[i]);
    }
  }

  private void testParseMLog(String storageGroup, int schemaRegionId, int expectedLineNum)
      throws IOException {
    testParseLog(
        IoTDBDescriptor.getInstance().getConfig().getSchemaDir()
            + File.separator
            + storageGroup
            + File.separator
            + schemaRegionId
            + File.separator
            + SchemaConstant.METADATA_LOG,
        expectedLineNum);
  }

  private void testParseLog(String path, int expectedNum) throws IOException {
    File file = new File("target" + File.separator + "tmp" + File.separator + "text.mlog");
    file.delete();

    MLogParser.main(
        new String[] {
          "-f", path, "-o", "target" + File.separator + "tmp" + File.separator + "text.mlog"
        });

    try (BufferedReader reader =
        new BufferedReader(
            new FileReader("target" + File.separator + "tmp" + File.separator + "text.mlog"))) {
      int lineNum = 0;
      List<String> lines = new ArrayList<>();
      String line;
      while ((line = reader.readLine()) != null) {
        lineNum++;
        lines.add(line);
      }
      if (lineNum != expectedNum) {
        for (String content : lines) {
          System.out.println(content);
        }
      }
      Assert.assertEquals(expectedNum, lineNum);
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }
}
