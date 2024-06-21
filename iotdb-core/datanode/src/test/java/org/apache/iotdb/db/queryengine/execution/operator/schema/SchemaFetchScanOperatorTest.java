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

package org.apache.iotdb.db.queryengine.execution.operator.schema;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class SchemaFetchScanOperatorTest {

  @Test
  public void testSchemaFetchResult() throws Exception {
    ISchemaRegion schemaRegion = mockSchemaRegion();

    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.**.status"));
    patternTree.appendPathPattern(new PartialPath("root.**.s1"));
    patternTree.constructTree();

    SchemaFetchScanOperator schemaFetchScanOperator =
        SchemaFetchScanOperator.ofSeries(
            null,
            null,
            patternTree,
            Collections.emptyMap(),
            schemaRegion,
            false,
            false,
            true,
            false);

    Assert.assertTrue(schemaFetchScanOperator.hasNext());

    TsBlock tsBlock = schemaFetchScanOperator.next();

    Assert.assertFalse(schemaFetchScanOperator.hasNext());

    Binary binary = tsBlock.getColumn(0).getBinary(0);
    InputStream inputStream = new ByteArrayInputStream(binary.getValues());
    Assert.assertEquals(1, ReadWriteIOUtils.readByte(inputStream));
    ISchemaTree schemaTree = ClusterSchemaTree.deserialize(inputStream);

    DeviceSchemaInfo deviceSchemaInfo =
        schemaTree.searchDeviceSchemaInfo(
            new PartialPath("root.sg.d2.a"), Arrays.asList("s1", "status"));
    Assert.assertTrue(deviceSchemaInfo.isAligned());
    List<IMeasurementSchema> measurementSchemaList = deviceSchemaInfo.getMeasurementSchemaList();
    Assert.assertEquals(2, measurementSchemaList.size());
    Assert.assertEquals(
        Arrays.asList("s1", "s2"),
        measurementSchemaList.stream()
            .map(IMeasurementSchema::getMeasurementId)
            .sorted()
            .collect(Collectors.toList()));

    Pair<List<MeasurementPath>, Integer> pair =
        schemaTree.searchMeasurementPaths(new PartialPath("root.sg.**.status"), 0, 0, false);
    Assert.assertEquals(3, pair.left.size());
    Assert.assertEquals(
        Arrays.asList("root.sg.d1.s2", "root.sg.d2.a.s2", "root.sg.d2.s2"),
        pair.left.stream().map(MeasurementPath::getFullPath).collect(Collectors.toList()));
  }

  private ISchemaRegion mockSchemaRegion() throws Exception {
    ISchemaRegion schemaRegion = Mockito.mock(ISchemaRegion.class);

    MeasurementPath d1s1 =
        new MeasurementPath(
            new PartialPath("root.sg.d1.s1"),
            new MeasurementSchema(
                "s1", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, null),
            false);
    MeasurementPath d1s2 =
        new MeasurementPath(
            new PartialPath("root.sg.d1.s2"),
            new MeasurementSchema(
                "s2", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, null),
            false);
    d1s2.setMeasurementAlias("status");

    MeasurementPath d2s1 =
        new MeasurementPath(
            new PartialPath("root.sg.d2.s1"),
            new MeasurementSchema(
                "s1", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, null),
            false);
    MeasurementPath d2s2 =
        new MeasurementPath(
            new PartialPath("root.sg.d2.s2"),
            new MeasurementSchema(
                "s2", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, null),
            false);
    d2s2.setMeasurementAlias("status");

    MeasurementPath d2as1 =
        new MeasurementPath(
            new PartialPath("root.sg.d2.a.s1"),
            new MeasurementSchema(
                "s1", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, null),
            true);
    MeasurementPath d2as2 =
        new MeasurementPath(
            new PartialPath("root.sg.d2.a.s2"),
            new MeasurementSchema(
                "s2", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, null),
            true);
    d2as2.setMeasurementAlias("status");

    ClusterSchemaTree clusterSchemaTree = new ClusterSchemaTree();
    clusterSchemaTree.appendSingleMeasurementPath(d1s2);
    clusterSchemaTree.appendSingleMeasurementPath(d2as2);
    clusterSchemaTree.appendSingleMeasurementPath(d2s2);
    clusterSchemaTree.appendSingleMeasurementPath(d1s1);
    clusterSchemaTree.appendSingleMeasurementPath(d2as1);
    clusterSchemaTree.appendSingleMeasurementPath(d2s1);

    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.**.status"));
    patternTree.appendPathPattern(new PartialPath("root.**.s1"));
    patternTree.constructTree();
    Mockito.when(
            schemaRegion.fetchSeriesSchema(
                patternTree, Collections.emptyMap(), false, false, true, false))
        .thenReturn(clusterSchemaTree);

    return schemaRegion;
  }
}
