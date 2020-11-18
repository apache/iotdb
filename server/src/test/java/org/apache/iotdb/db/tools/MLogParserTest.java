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
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MLogParserTest {

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    File file = new File("target" + File.separator
      + "tmp"  + File.separator + "text.mlog");
    file.deleteOnExit();
    file = new File("target" + File.separator
      + "tmp"  + File.separator + "text.snapshot");
    file.deleteOnExit();
  }

  public void prepareData() {
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
  }

  @Test
  public void testParseMLog() throws IOException {
    prepareData();
    IoTDB.metaManager.flushAllMlogForTest();

    try {
      MLogParser.parseFromFile(IoTDBDescriptor.getInstance().getConfig().getSchemaDir()
        + File.separator + MetadataConstant.METADATA_LOG,
        "target" + File.separator + "tmp"  + File.separator + "text.mlog");
    } catch (IOException e) {
      e.printStackTrace();
    }

    try (BufferedReader reader = new BufferedReader(new FileReader("target" + File.separator
      + "tmp"  + File.separator + "text.mlog"))) {
      int lineNum = 0;
      while (reader.readLine() != null) {
        lineNum++;
      }
      Assert.assertEquals(lineNum, 108);
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testParseSnapshot() {
    prepareData();
    IoTDB.metaManager.createMTreeSnapshot();

    try {
      MLogParser.parseFromFile(IoTDBDescriptor.getInstance().getConfig().getSchemaDir()
          + File.separator + MetadataConstant.MTREE_SNAPSHOT,
        "target" + File.separator + "tmp"  + File.separator + "text.snapshot");
    } catch (IOException e) {
      e.printStackTrace();
    }

    try (BufferedReader reader = new BufferedReader(new FileReader("target" + File.separator
      + "tmp"  + File.separator + "text.snapshot"))) {
      int lineNum = 0;
      while (reader.readLine() != null) {
        lineNum++;
      }
      Assert.assertEquals(lineNum, 113);
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }
}
