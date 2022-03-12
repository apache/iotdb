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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.sys.ActivateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTemplatePlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.tools.mlog.MLogParser;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MLogParserTest {

  private String[] storageGroups = new String[] {"root.sg0", "root.sg1", "root.sgcc", "root.sg"};

  /*
   * For root.sg0, we prepare 50 CreateTimeseriesPlan.
   * For root.sg1, we prepare 50 CreateTimeseriesPlan, 1 DeleteTimeseriesPlan, 1 ChangeTagOffsetPlan and 1 ChangeAliasPlan.
   * For root.sgcc, we prepare 1 SetTTLPlan.
   * For root.sg, we prepare 1 SetTemplatePlan, 1 AutoCreateDevicePlan and 1 ActivateTemplatePlan.
   *
   * For root.ln.cc, we create it and then delete it, thus there's no mlog of root.ln.cc.
   * There' still 1 CreateTemplatePlan in template_log.bin
   *
   * */
  private int[] mlogLineNum = new int[] {50, 53, 1, 3};

  /*
   * For root.sg0, we prepare 5 device and 10 measurement per device, thus there are 1 + 5 + 5 * 10 = 56 MNodes .
   * For root.sg1, we prepare 5 device and 10 measurement per device and then delete 1 measurement, thus there are 1 + 5 + 5 * 10 - 1 = 55 MNodes .
   * For root.sgcc, there is only 1 StorageGroupMNode.
   * For root.sg, we prepare 1 device, thus there are 1 + 1 = 2 MNodes.
   *
   * For root.ln.cc, we create it and then delete it, thus there's no snapshot of root.ln.cc.
   *
   * */
  private int[] snapshotLineNum = new int[] {56, 55, 1, 2};

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
            IoTDB.metaManager.createTimeseries(plan);
          } catch (MetadataException e) {
            e.printStackTrace();
          }
        }
      }
    }

    try {
      IoTDB.metaManager.setStorageGroup(new PartialPath("root.ln.cc"));
      IoTDB.metaManager.setStorageGroup(new PartialPath("root.sgcc"));
      IoTDB.metaManager.setTTL(new PartialPath("root.sgcc"), 1234L);
      IoTDB.metaManager.deleteTimeseries(new PartialPath("root.sg1.device1.s1"));
      List<PartialPath> paths = new ArrayList<>();
      paths.add(new PartialPath("root.ln.cc"));
      IoTDB.metaManager.deleteStorageGroups(paths);
      Map<String, String> tags = new HashMap<String, String>();
      tags.put("tag1", "value1");
      IoTDB.metaManager.addTags(tags, new PartialPath("root.sg1.device1.s2"));
      IoTDB.metaManager.changeAlias(new PartialPath("root.sg1.device1.s3"), "hello");
    } catch (MetadataException | IOException e) {
      e.printStackTrace();
    }

    try {
      IoTDB.metaManager.setStorageGroup(new PartialPath("root.sg"));
      IoTDB.metaManager.createSchemaTemplate(genCreateSchemaTemplatePlan());
      SetTemplatePlan setTemplatePlan = new SetTemplatePlan("template1", "root.sg");
      IoTDB.metaManager.setSchemaTemplate(setTemplatePlan);
      IoTDB.metaManager.setUsingSchemaTemplate(
          new ActivateTemplatePlan(new PartialPath("root.sg.d1")));
    } catch (MetadataException e) {
      e.printStackTrace();
    }
  }

  private CreateTemplatePlan genCreateSchemaTemplatePlan() {
    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList("s11"));
    measurementList.add(Collections.singletonList("s12"));

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Collections.singletonList(TSDataType.INT64));
    dataTypeList.add(Collections.singletonList(TSDataType.DOUBLE));

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Collections.singletonList(TSEncoding.RLE));
    encodingList.add(Collections.singletonList(TSEncoding.GORILLA));

    List<List<CompressionType>> compressionTypes = new ArrayList<>();
    compressionTypes.add(Collections.singletonList(CompressionType.SNAPPY));
    compressionTypes.add(Collections.singletonList(CompressionType.SNAPPY));

    List<String> schemaNames = new ArrayList<>();
    schemaNames.add("s11");
    schemaNames.add("s12");

    return new CreateTemplatePlan(
        "template1", schemaNames, measurementList, dataTypeList, encodingList, compressionTypes);
  }

  @Test
  public void testMLogParser() throws IOException {
    prepareData();
    testDeletedStorageGroup("root.ln.cc");

    File file;

    IoTDB.metaManager.flushAllMlogForTest();
    for (int i = 0; i < storageGroups.length; i++) {
      testParseMLog(storageGroups[i], mlogLineNum[i]);
      file = new File("target" + File.separator + "tmp" + File.separator + "text.mlog");
      file.delete();
    }

    IoTDB.metaManager.createMTreeSnapshot();
    for (int i = 0; i < storageGroups.length; i++) {
      testParseMLog(storageGroups[i], 0);
      file = new File("target" + File.separator + "tmp" + File.separator + "text.mlog");
      file.delete();
      testParseSnapshot(storageGroups[i], snapshotLineNum[i]);
      file = new File("target" + File.separator + "tmp" + File.separator + "text.snapshot");
      file.delete();
    }

    testParseTemplateLogFile();
    file = new File("target" + File.separator + "tmp" + File.separator + "text.mlog");
    file.delete();
  }

  private void testDeletedStorageGroup(String storageGroup) {
    File storageGroupDir =
        new File(
            IoTDBDescriptor.getInstance().getConfig().getSchemaDir()
                + File.separator
                + storageGroup);
    Assert.assertFalse(storageGroupDir.exists());
  }

  private void testParseMLog(String storageGroup, int expectedLineNum) throws IOException {
    try {
      MLogParser.parseFromFile(
          IoTDBDescriptor.getInstance().getConfig().getSchemaDir()
              + File.separator
              + storageGroup
              + File.separator
              + MetadataConstant.METADATA_LOG,
          "target" + File.separator + "tmp" + File.separator + "text.mlog");
    } catch (IOException e) {
      e.printStackTrace();
    }

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
      if (lineNum != expectedLineNum) {
        for (String content : lines) {
          System.out.println(content);
        }
      }
      Assert.assertEquals(expectedLineNum, lineNum);
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }

  private void testParseSnapshot(String storageGroup, int expectedLineNum) {
    try {
      MLogParser.parseFromFile(
          IoTDBDescriptor.getInstance().getConfig().getSchemaDir()
              + File.separator
              + storageGroup
              + File.separator
              + MetadataConstant.MTREE_SNAPSHOT,
          "target" + File.separator + "tmp" + File.separator + "text.snapshot");
    } catch (IOException e) {
      e.printStackTrace();
    }

    try (BufferedReader reader =
        new BufferedReader(
            new FileReader("target" + File.separator + "tmp" + File.separator + "text.snapshot"))) {
      int lineNum = 0;
      List<String> lines = new ArrayList<>();
      String line;
      while ((line = reader.readLine()) != null) {
        lineNum++;
        lines.add(line);
      }
      if (lineNum != expectedLineNum) {
        for (String content : lines) {
          System.out.println(content);
        }
      }
      Assert.assertEquals(expectedLineNum, lineNum);
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }

  private void testParseTemplateLogFile() throws IOException {
    try {
      MLogParser.parseFromFile(
          IoTDBDescriptor.getInstance().getConfig().getSchemaDir()
              + File.separator
              + MetadataConstant.TEMPLATE_FILE,
          "target" + File.separator + "tmp" + File.separator + "text.mlog");
    } catch (IOException e) {
      e.printStackTrace();
    }

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
      if (lineNum != 1) {
        for (String content : lines) {
          System.out.println(content);
        }
      }
      Assert.assertEquals(1, lineNum);
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }
}
