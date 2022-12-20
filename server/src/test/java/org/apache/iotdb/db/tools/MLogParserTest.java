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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.LocalSchemaProcessor;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.tools.schema.MLogParser;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

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

  private String[] storageGroups = new String[] {"root.sg0", "root.sg1", "root.sgcc", "root.sg"};
  private int[] storageGroupIndex = new int[] {0, 1, 3, 4};

  /*
   * For root.sg0, we prepare 50 CreateTimeseriesPlan.
   * For root.sg1, we prepare 50 CreateTimeseriesPlan, 1 DeleteTimeseriesPlan, 1 ChangeTagOffsetPlan and 1 ChangeAliasPlan.
   * For root.sgcc, we prepare 0 plans on timeseries or device or template.
   * For root.sg, we prepare none schema plan.
   *
   * For root.ln.cc, we create it and then delete it, thus there's no mlog of root.ln.cc.
   * There' still 1 CreateTemplatePlan in template_log.bin
   *
   * */
  private int[] mlogLineNum = new int[] {50, 53, 0, 0};

  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    File file = new File("target" + File.separator + "tmp" + File.separator + "text.mlog");
    file.deleteOnExit();
    file = new File("target" + File.separator + "tmp" + File.separator + "text.snapshot");
    file.deleteOnExit();
  }

  private void prepareData() {
    // prepare data
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 5; j++) {
        for (int k = 0; k < 10; k++) {
          CreateTimeSeriesPlan plan = new CreateTimeSeriesPlan();
          try {
            plan.setPath(new PartialPath("root.sg" + i + "." + "device" + j + "." + "s" + k));
            plan.setDataType(TSDataType.INT32);
            plan.setEncoding(TSEncoding.PLAIN);
            plan.setCompressor(CompressionType.GZIP);
            LocalSchemaProcessor.getInstance().createTimeseries(plan);
          } catch (MetadataException e) {
            e.printStackTrace();
          }
        }
      }
    }

    try {
      LocalSchemaProcessor.getInstance().setStorageGroup(new PartialPath("root.ln.cc"));
      LocalSchemaProcessor.getInstance().setStorageGroup(new PartialPath("root.sgcc"));
      LocalSchemaProcessor.getInstance().setTTL(new PartialPath("root.sgcc"), 1234L);
      LocalSchemaProcessor.getInstance().deleteTimeseries(new PartialPath("root.sg1.device1.s1"));
      List<PartialPath> paths = new ArrayList<>();
      paths.add(new PartialPath("root.ln.cc"));
      LocalSchemaProcessor.getInstance().deleteStorageGroups(paths);
      Map<String, String> tags = new HashMap<String, String>();
      tags.put("tag1", "value1");
      LocalSchemaProcessor.getInstance().addTags(tags, new PartialPath("root.sg1.device1.s2"));
      LocalSchemaProcessor.getInstance()
          .changeAlias(new PartialPath("root.sg1.device1.s3"), "hello");
    } catch (MetadataException | IOException e) {
      e.printStackTrace();
    }

    try {
      LocalSchemaProcessor.getInstance().setStorageGroup(new PartialPath("root.sg"));
    } catch (MetadataException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testMLogParser() throws Exception {
    prepareData();
    testNonExistingStorageGroupDir("root.ln.cc");

    LocalSchemaProcessor.getInstance().forceMlog();

    for (int i = 0; i < storageGroups.length; i++) {
      testParseMLog(storageGroups[i], storageGroupIndex[i], mlogLineNum[i]);
    }
  }

  private void testNonExistingStorageGroupDir(String storageGroup) {
    File storageGroupDir =
        new File(
            IoTDBDescriptor.getInstance().getConfig().getSchemaDir()
                + File.separator
                + storageGroup);
    Assert.assertFalse(storageGroupDir.exists());
  }

  private void testParseMLog(String storageGroup, int storageGroupId, int expectedLineNum)
      throws IOException {
    testParseLog(
        IoTDBDescriptor.getInstance().getConfig().getSchemaDir()
            + File.separator
            + storageGroup
            + File.separator
            + storageGroupId
            + File.separator
            + MetadataConstant.METADATA_LOG,
        expectedLineNum);
  }

  private void testParseLog(String path, int expectedNum) throws IOException {
    File file = new File("target" + File.separator + "tmp" + File.separator + "text.mlog");
    file.delete();

    MLogParser.parseFromFile(
        path, "target" + File.separator + "tmp" + File.separator + "text.mlog");

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
