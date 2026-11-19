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

package org.apache.iotdb.db.queryengine.plan.analyze.schema;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TreeDeviceSchemaCacheManager;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;
import org.apache.iotdb.db.schemaengine.template.TemplateInternalRPCUtil;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_MATCH_PATTERN;

public class ClusterSchemaFetcherTest {

  private Template template;
  private int previousDataNodeId;

  @Before
  public void setUp() throws Exception {
    // ClusterSchemaFetcher creates the Coordinator, whose query id generator needs a DataNode id.
    previousDataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
    template =
        new Template(
            "t1",
            Arrays.asList("s1", "s2"),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.INT32),
            Arrays.asList(TSEncoding.RLE, TSEncoding.RLE),
            Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY));
    template.setId(1);
    ClusterTemplateManager.getInstance()
        .addTemplateSetInfo(
            TemplateInternalRPCUtil.generateAddTemplateSetInfoBytes(template, "root.sg1"));
  }

  @After
  public void tearDown() {
    TreeDeviceSchemaCacheManager.getInstance().cleanUp();
    ClusterTemplateManager.getInstance().clear();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(previousDataNodeId);
  }

  @Test
  public void testFetchTemplateDeviceSchemaFromCache() throws Exception {
    final ClusterSchemaTree cached = new ClusterSchemaTree();
    cached.appendTemplateDevice(new PartialPath("root.sg1.d1"), false, 1, template);
    cached.setDatabases(Collections.singleton("root.sg1"));
    TreeDeviceSchemaCacheManager.getInstance().put(cached);

    final PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendFullPath(new PartialPath("root.sg1.d1.s1"));
    patternTree.appendFullPath(new PartialPath("root.sg1.d1.s2"));

    // A cache hit returns without a remote fetch. A miss falls through to
    // fetchSchemaOfPreciseMatchOrPreciseDeviceUsingTemplate, which fails here without a cluster.
    final ClusterSchemaTree fetched =
        ClusterSchemaFetcher.getInstance()
            .fetchSchema(patternTree, true, new MPPQueryContext(new QueryId("test")), true);

    final List<String> paths =
        fetched.searchMeasurementPaths(ALL_MATCH_PATTERN).left.stream()
            .map(MeasurementPath::getFullPath)
            .sorted()
            .collect(Collectors.toList());
    Assert.assertEquals(Arrays.asList("root.sg1.d1.s1", "root.sg1.d1.s2"), paths);
    Assert.assertEquals(Collections.singleton("root.sg1"), fetched.getDatabases());
  }
}
