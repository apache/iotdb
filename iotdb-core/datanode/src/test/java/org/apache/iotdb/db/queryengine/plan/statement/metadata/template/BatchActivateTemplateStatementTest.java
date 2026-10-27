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

package org.apache.iotdb.db.queryengine.plan.statement.metadata.template;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalBatchActivateTemplateStatement;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;
import org.apache.iotdb.db.schemaengine.template.TemplateInternalRPCUtil;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class BatchActivateTemplateStatementTest {

  private Template template1;
  private Template template2;

  @Before
  public void setUp() throws Exception {
    final ClusterTemplateManager templateManager = ClusterTemplateManager.getInstance();
    templateManager.clear();

    template1 = createTemplate("t1", 1, Arrays.asList("s1", "s2"));
    templateManager.addTemplateSetInfo(
        TemplateInternalRPCUtil.generateAddTemplateSetInfoBytes(template1, "root.sg.v1"));

    template2 = createTemplate("t2", 2, Collections.singletonList("s3"));
    templateManager.addTemplateSetInfo(
        TemplateInternalRPCUtil.generateAddTemplateSetInfoBytes(template2, "root.sg.v2"));
  }

  @After
  public void tearDown() {
    ClusterTemplateManager.getInstance().clear();
  }

  @Test
  public void testGetPathsUsesTheTemplateOfEachDevice() throws IllegalPathException {
    final BatchActivateTemplateStatement statement =
        new BatchActivateTemplateStatement(
            Arrays.asList(
                new PartialPath("root.sg.v1.d1"),
                new PartialPath("root.sg.v2.d1"),
                // no template is set on this path, so it contributes no path
                new PartialPath("root.sg.v3.d1")));

    final List<PartialPath> paths = statement.getPaths();

    // s1 and s2 belong to template1, s3 belongs to template2; each path appears exactly once
    Assert.assertEquals(3, paths.size());
    Assert.assertEquals(
        new HashSet<>(
            Arrays.asList(
                new PartialPath("root.sg.v1.d1.s1"),
                new PartialPath("root.sg.v1.d1.s2"),
                new PartialPath("root.sg.v2.d1.s3"))),
        new HashSet<>(paths));
  }

  @Test
  public void testInternalGetPathsUsesTheTemplateOfEachDevice() throws IllegalPathException {
    final Map<PartialPath, Pair<Template, PartialPath>> deviceMap = new HashMap<>();
    deviceMap.put(
        new PartialPath("root.sg.v1.d1"), new Pair<>(template1, new PartialPath("root.sg.v1")));
    deviceMap.put(
        new PartialPath("root.sg.v2.d1"), new Pair<>(template2, new PartialPath("root.sg.v2")));

    final InternalBatchActivateTemplateStatement statement =
        new InternalBatchActivateTemplateStatement(deviceMap);

    final List<PartialPath> paths = statement.getPaths();

    Assert.assertEquals(3, paths.size());
    Assert.assertEquals(
        new HashSet<>(
            Arrays.asList(
                new PartialPath("root.sg.v1.d1.s1"),
                new PartialPath("root.sg.v1.d1.s2"),
                new PartialPath("root.sg.v2.d1.s3"))),
        new HashSet<>(paths));
  }

  private static Template createTemplate(
      final String name, final int id, final List<String> measurements) throws Exception {
    final Template template =
        new Template(
            name,
            measurements,
            Collections.nCopies(measurements.size(), TSDataType.DOUBLE),
            Collections.nCopies(measurements.size(), TSEncoding.RLE),
            Collections.nCopies(measurements.size(), CompressionType.SNAPPY));
    template.setId(id);
    return template;
  }
}
