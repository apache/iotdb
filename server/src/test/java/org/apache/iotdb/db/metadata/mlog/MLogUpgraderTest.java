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
package org.apache.iotdb.db.metadata.mlog;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.logfile.MLogTxtWriter;
import org.apache.iotdb.db.metadata.logfile.MLogUpgrader;
import org.apache.iotdb.db.metadata.tag.TagLogFile;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MLogUpgraderTest {

  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testUpgrade() throws IOException, MetadataException {
    MManager manager = IoTDB.metaManager;
    manager.clear();

    String schemaDir = IoTDBDescriptor.getInstance().getConfig().getSchemaDir();

    MLogTxtWriter mLogTxtWriter = new MLogTxtWriter(schemaDir, MetadataConstant.METADATA_TXT_LOG);
    mLogTxtWriter.createTimeseries(
        new CreateTimeSeriesPlan(
            new PartialPath("root.sg.d.s"),
            TSDataType.BOOLEAN,
            TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED,
            null,
            null,
            null,
            null),
        0);
    mLogTxtWriter.close();

    TagLogFile tagLogFile = new TagLogFile(schemaDir, MetadataConstant.TAG_LOG);
    Map<String, String> tags = new HashMap<>();
    tags.put("name", "IoTDB");
    Map<String, String> attributes = new HashMap<>();
    attributes.put("type", "tsDB");
    tagLogFile.write(tags, attributes, 0);
    tagLogFile.close();

    File mLog = new File(schemaDir + File.separator + MetadataConstant.METADATA_LOG);
    if (mLog.exists()) {
      mLog.delete();
    }

    MLogUpgrader.upgradeMLog();

    manager.init();
    ShowTimeSeriesPlan plan =
        new ShowTimeSeriesPlan(new PartialPath("root.**"), true, "name", "DB", 0, 0, false);
    ShowTimeSeriesResult result = manager.showTimeseries(plan, null).get(0);
    Assert.assertEquals("root.sg.d.s", result.getName());
    Assert.assertEquals(tags, result.getTag());
    Assert.assertEquals(attributes, result.getAttribute());
  }
}
