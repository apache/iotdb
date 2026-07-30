/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.source;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.pipe.source.schemaregion.PipeTreeStatementToPlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.MeasurementGroup;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalCreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.BatchActivateTemplateStatement;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class PipeTreeStatementToPlanVisitorTest {
  @Test
  public void testInternalCreateMultiTimeSeries() throws IllegalPathException {
    final MeasurementGroup originalMeasurementGroup = new MeasurementGroup();
    originalMeasurementGroup.addMeasurement(
        "s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);
    originalMeasurementGroup.addProps(Collections.emptyMap());
    originalMeasurementGroup.addAlias("a1");
    originalMeasurementGroup.addTags(Collections.emptyMap());
    originalMeasurementGroup.addAttributes(Collections.emptyMap());
    originalMeasurementGroup.addMeasurement(
        "s2", TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.SNAPPY);
    originalMeasurementGroup.addProps(Collections.emptyMap());
    originalMeasurementGroup.addAlias("a2");
    originalMeasurementGroup.addTags(Collections.emptyMap());
    originalMeasurementGroup.addAttributes(Collections.emptyMap());

    final Map<PartialPath, Pair<Boolean, MeasurementGroup>> testMap =
        new HashMap<PartialPath, Pair<Boolean, MeasurementGroup>>() {
          {
            put(new PartialPath("root.db.device"), new Pair<>(false, originalMeasurementGroup));
            put(new PartialPath("root.db1.device"), new Pair<>(false, originalMeasurementGroup));
          }
        };

    Assert.assertEquals(
        testMap,
        new PipeTreeStatementToPlanVisitor()
            .visitInternalCreateMultiTimeSeries(
                new InternalCreateMultiTimeSeriesStatement(testMap), null)
            .getDeviceMap());
  }

  @Test
  public void testBatchActivateTemplate() throws IllegalPathException {
    final List<PartialPath> templatePaths =
        Arrays.asList(new PartialPath("root.db.device"), new PartialPath("root.db.device2"));
    Assert.assertEquals(
        new HashSet<>(templatePaths),
        new PipeTreeStatementToPlanVisitor()
            .visitBatchActivateTemplate(new BatchActivateTemplateStatement(templatePaths), null)
            .getTemplateActivationMap()
            .keySet());
  }
}
