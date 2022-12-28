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
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.sys.ActivateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.AppendTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DeactivateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.DropTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.PruneTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.SetTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.UnsetTemplatePlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.tools.mlog.MLogLoader;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MLogLoaderTest {

  File mLog = new File("target" + File.separator + "tmp" + File.separator + "mlog.bin.bak");
  File tLog = new File("target" + File.separator + "tmp" + File.separator + "tlog.txt.bak");

  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    mLog.deleteOnExit();
    tLog.deleteOnExit();
    EnvironmentUtils.cleanEnv();
  }

  private void prepareMLog() throws Exception {
    // create time series
    CreateTimeSeriesPlan createTimeSeriesPlan1 = new CreateTimeSeriesPlan();
    createTimeSeriesPlan1.setPath(new PartialPath("root.sg1.device1.s1"));
    createTimeSeriesPlan1.setDataType(TSDataType.INT32);
    createTimeSeriesPlan1.setEncoding(TSEncoding.PLAIN);
    createTimeSeriesPlan1.setCompressor(CompressionType.GZIP);
    createTimeSeriesPlan1.setAlias("measurement");
    createTimeSeriesPlan1.setAttributes(
        new HashMap<String, String>() {
          {
            put("attr1", "a1");
            put("attr2", "a2");
          }
        });
    createTimeSeriesPlan1.setTags(
        new HashMap<String, String>() {
          {
            put("tag1", "t1");
            put("tag2", "t2");
          }
        });
    IoTDB.metaManager.createTimeseries(createTimeSeriesPlan1);
    CreateTimeSeriesPlan createTimeSeriesPlan2 = new CreateTimeSeriesPlan();
    createTimeSeriesPlan2.setPath(new PartialPath("root.sg2.device1.s1"));
    createTimeSeriesPlan2.setDataType(TSDataType.BOOLEAN);
    createTimeSeriesPlan2.setEncoding(TSEncoding.PLAIN);
    createTimeSeriesPlan2.setCompressor(CompressionType.GZIP);
    IoTDB.metaManager.createTimeseries(createTimeSeriesPlan2);
    CreateTimeSeriesPlan createTimeSeriesPlan3 = new CreateTimeSeriesPlan();
    createTimeSeriesPlan3.setPath(new PartialPath("root.sg1.device1.s3"));
    createTimeSeriesPlan3.setDataType(TSDataType.DOUBLE);
    createTimeSeriesPlan3.setEncoding(TSEncoding.PLAIN);
    createTimeSeriesPlan3.setCompressor(CompressionType.GZIP);
    IoTDB.metaManager.createTimeseries(createTimeSeriesPlan3);
    CreateTimeSeriesPlan createTimeSeriesPlan4 = new CreateTimeSeriesPlan();
    createTimeSeriesPlan4.setPath(new PartialPath("root.sg1.device1.s4"));
    createTimeSeriesPlan4.setDataType(TSDataType.DOUBLE);
    createTimeSeriesPlan4.setEncoding(TSEncoding.PLAIN);
    createTimeSeriesPlan4.setCompressor(CompressionType.GZIP);
    IoTDB.metaManager.createTimeseries(createTimeSeriesPlan4);
    IoTDB.metaManager.createAlignedTimeSeries(
        new PartialPath("root.laptop.d1.aligned_device"),
        Arrays.asList("s3", "s4", "s5"),
        Arrays.asList(
            TSDataType.valueOf("FLOAT"), TSDataType.valueOf("INT64"), TSDataType.valueOf("INT32")),
        Arrays.asList(
            TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE")),
        Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY, CompressionType.SNAPPY));
    // delete timeseries
    IoTDB.metaManager.deleteTimeseries(new PartialPath("root.sg2.device1.s1"));
    // set storage group
    IoTDB.metaManager.setStorageGroup(new PartialPath("root.ln.cc1"));
    IoTDB.metaManager.setStorageGroup(new PartialPath("root.ln.cc2"));
    IoTDB.metaManager.setStorageGroup(new PartialPath("root.ln.cc3"));
    IoTDB.metaManager.setStorageGroup(new PartialPath("root.sgcc"));
    // delete storage group
    IoTDB.metaManager.deleteStorageGroups(
        Arrays.asList(new PartialPath("root.ln.cc1"), new PartialPath("root.ln.cc2")));
    // ttl
    IoTDB.metaManager.setTTL(new PartialPath("root.sgcc"), 1234L);
    // tag attribute
    IoTDB.metaManager.renameTagOrAttributeKey(
        "tag1", "newTag1", new PartialPath("root.sg1.device1.s1"));
    IoTDB.metaManager.setTagsOrAttributesValue(
        new HashMap<String, String>() {
          {
            put("newTag1", "newT1");
            put("attr1", "newA1");
          }
        },
        new PartialPath("root.sg1.device1.s1"));
    IoTDB.metaManager.dropTagsOrAttributes(
        new HashSet<String>() {
          {
            add("attr2");
          }
        },
        new PartialPath("root.sg1.device1.s1"));
    IoTDB.metaManager.addTags(
        new HashMap<String, String>() {
          {
            put("tag3", "t3");
          }
        },
        new PartialPath("root.sg1.device1.s1"));
    IoTDB.metaManager.addAttributes(
        new HashMap<String, String>() {
          {
            put("attr3", "a3");
          }
        },
        new PartialPath("root.sg1.device1.s1"));
    // alter time series -> change alias and change offset
    IoTDB.metaManager.upsertTagsAndAttributes(
        "newAlias1",
        new HashMap<String, String>() {
          {
            put("tag4", "t4");
          }
        },
        new HashMap<String, String>() {
          {
            put("attr4", "a4");
          }
        },
        new PartialPath("root.sg1.device1.s1"));
    IoTDB.metaManager.upsertTagsAndAttributes(
        "newAlias2",
        new HashMap<String, String>() {
          {
            put("tag4", "t4");
          }
        },
        new HashMap<String, String>() {
          {
            put("attr4", "a4");
          }
        },
        new PartialPath("root.sg1.device1.s3"));
    // create template
    IoTDB.metaManager.createSchemaTemplate(genUnalignedCreateSchemaTemplatePlan("template1"));
    IoTDB.metaManager.createSchemaTemplate(genAlignedCreateSchemaTemplatePlan("template2"));
    IoTDB.metaManager.createSchemaTemplate(genAlignedCreateSchemaTemplatePlan("template3"));
    // append template
    IoTDB.metaManager.appendSchemaTemplate(
        new AppendTemplatePlan(
            "template1",
            false,
            Arrays.asList("a1", "a2"),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.FLOAT),
            Arrays.asList(TSEncoding.RLE, TSEncoding.RLE),
            Arrays.asList(CompressionType.SNAPPY, CompressionType.LZ4)));
    IoTDB.metaManager.appendSchemaTemplate(
        new AppendTemplatePlan(
            "template2",
            true,
            Arrays.asList("a1", "a2"),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.FLOAT),
            Arrays.asList(TSEncoding.RLE, TSEncoding.RLE),
            Arrays.asList(CompressionType.SNAPPY, CompressionType.LZ4)));
    // prune template
    IoTDB.metaManager.pruneSchemaTemplate(
        new PruneTemplatePlan("template1", Collections.singletonList("s11")));
    // set template
    IoTDB.metaManager.setSchemaTemplate(new SetTemplatePlan("template1", "root.sgcc.d1"));
    IoTDB.metaManager.setSchemaTemplate(new SetTemplatePlan("template2", "root.sgcc.d2"));
    IoTDB.metaManager.setSchemaTemplate(new SetTemplatePlan("template3", "root.sgcc.d3"));
    // unset template
    IoTDB.metaManager.unsetSchemaTemplate(new UnsetTemplatePlan("root.sgcc.d3", "template3"));
    // drop template
    IoTDB.metaManager.dropSchemaTemplate(new DropTemplatePlan("template3"));
    // activate template
    IoTDB.metaManager.setUsingSchemaTemplate(
        new ActivateTemplatePlan(new PartialPath("root.sgcc.d1")));
    IoTDB.metaManager.setUsingSchemaTemplate(
        new ActivateTemplatePlan(new PartialPath("root.sgcc.d2")));
    // deactivate template
    DeactivateTemplatePlan deactivateTemplatePlan =
        new DeactivateTemplatePlan("template2", new PartialPath("root.sgcc.d2"));
    deactivateTemplatePlan.setPaths(Collections.singletonList(new PartialPath("root.sgcc.d2")));
    IoTDB.metaManager.deactivateSchemaTemplate(deactivateTemplatePlan);
  }

  private CreateTemplatePlan genUnalignedCreateSchemaTemplatePlan(String name) {
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
        name, schemaNames, measurementList, dataTypeList, encodingList, compressionTypes);
  }

  private CreateTemplatePlan genAlignedCreateSchemaTemplatePlan(String name) {
    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Arrays.asList("s11", "s12"));

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Arrays.asList(TSDataType.INT64, TSDataType.DOUBLE));

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Arrays.asList(TSEncoding.RLE, TSEncoding.GORILLA));

    List<List<CompressionType>> compressionTypes = new ArrayList<>();
    compressionTypes.add(Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY));

    return new CreateTemplatePlan(
        name, measurementList, dataTypeList, encodingList, compressionTypes);
  }

  @Test
  public void testWithTagFile() throws Exception {
    prepareMLog();
    List<ShowTimeSeriesResult> expectedTSResult =
        IoTDB.metaManager.showTimeseries(
            new ShowTimeSeriesPlan(new PartialPath("root.**")), new QueryContext());
    Map<PartialPath, Long> expectedSgTTL = IoTDB.metaManager.getStorageGroupsTTL();
    Set<String> expectedTemplates = IoTDB.metaManager.getAllTemplates();
    IoTDB.metaManager.flushAllMlogForTest();
    new File(
            IoTDBDescriptor.getInstance().getConfig().getSchemaDir()
                + File.separator
                + MetadataConstant.METADATA_LOG)
        .renameTo(mLog);
    new File(
            IoTDBDescriptor.getInstance().getConfig().getSchemaDir()
                + File.separator
                + MetadataConstant.TAG_LOG)
        .renameTo(tLog);
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.restartDaemon();
    String[] args = new String[] {"-mlog", mLog.getAbsolutePath(), "-tlog", tLog.getAbsolutePath()};
    MLogLoader.main(args);
    List<ShowTimeSeriesResult> actualTSResult =
        IoTDB.metaManager.showTimeseries(
            new ShowTimeSeriesPlan(new PartialPath("root.**")), new QueryContext());
    Map<PartialPath, Long> actualSgTTL = IoTDB.metaManager.getStorageGroupsTTL();
    Set<String> actualTemplates = IoTDB.metaManager.getAllTemplates();

    Assert.assertEquals(expectedTSResult, actualTSResult);
    Assert.assertEquals(expectedSgTTL, actualSgTTL);
    Assert.assertEquals(expectedTemplates, actualTemplates);
  }

  @Test
  public void testWithoutTagFile() throws Exception {
    prepareMLog();
    List<ShowTimeSeriesResult> expectedTSResult =
        IoTDB.metaManager.showTimeseries(
            new ShowTimeSeriesPlan(new PartialPath("root.**")), new QueryContext());
    Map<PartialPath, Long> expectedSgTTL = IoTDB.metaManager.getStorageGroupsTTL();
    Set<String> expectedTemplates = IoTDB.metaManager.getAllTemplates();
    IoTDB.metaManager.flushAllMlogForTest();
    new File(
            IoTDBDescriptor.getInstance().getConfig().getSchemaDir()
                + File.separator
                + MetadataConstant.METADATA_LOG)
        .renameTo(mLog);
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.restartDaemon();
    String[] args = new String[] {"-mlog", mLog.getAbsolutePath()};
    MLogLoader.main(args);
    List<ShowTimeSeriesResult> actualTSResult =
        IoTDB.metaManager.showTimeseries(
            new ShowTimeSeriesPlan(new PartialPath("root.**")), new QueryContext());
    Map<PartialPath, Long> actualSgTTL = IoTDB.metaManager.getStorageGroupsTTL();
    Set<String> actualTemplates = IoTDB.metaManager.getAllTemplates();

    Assert.assertNotEquals(expectedTSResult, actualTSResult);
    for (ShowTimeSeriesResult showTimeSeriesResult : expectedTSResult) {
      showTimeSeriesResult.setTags(Collections.emptyMap());
      showTimeSeriesResult.setAttributes(Collections.emptyMap());
    }
    Assert.assertEquals(expectedTSResult, actualTSResult);
    Assert.assertEquals(expectedSgTTL, actualSgTTL);
    Assert.assertEquals(expectedTemplates, actualTemplates);
  }
}
