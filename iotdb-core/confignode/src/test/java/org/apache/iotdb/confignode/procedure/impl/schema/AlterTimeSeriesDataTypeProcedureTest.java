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

package org.apache.iotdb.confignode.procedure.impl.schema;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.commons.schema.tree.AlterTimeSeriesOperationType;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TreeDeviceSchemaCacheManager;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class AlterTimeSeriesDataTypeProcedureTest {

  @Test
  public void serializeDeserializeTest() throws IllegalPathException, IOException {
    String queryId = "1";

    MeasurementPath measurementPath = getMeasurementPath();
    AlterTimeSeriesDataTypeProcedure alterTimeSeriesDataTypeProcedure =
        new AlterTimeSeriesDataTypeProcedure(
            queryId,
            measurementPath,
            AlterTimeSeriesOperationType.ALTER_DATA_TYPE.getTypeValue(),
            TSDataType.FLOAT,
            false);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    alterTimeSeriesDataTypeProcedure.serialize(dataOutputStream);

    ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(
        ProcedureType.ALTER_TIMESERIES_DATATYPE_PROCEDURE.getTypeCode(), byteBuffer.getShort());

    AlterTimeSeriesDataTypeProcedure deserializedProcedure =
        new AlterTimeSeriesDataTypeProcedure(false);
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(queryId, deserializedProcedure.getQueryId());
    Assert.assertEquals("root.sg1.d1.s1", deserializedProcedure.getmeasurementPath().getFullPath());
  }

  private static MeasurementPath getMeasurementPath() throws IllegalPathException {
    final ClusterSchemaTree clusterSchemaTree = new ClusterSchemaTree();
    final Template template1 =
        new Template(
            "t1",
            Arrays.asList("s1", "s2"),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.INT32),
            Arrays.asList(TSEncoding.RLE, TSEncoding.RLE),
            Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY));
    template1.setId(1);
    final Template template2 =
        new Template(
            "t2",
            Arrays.asList("t1", "t2", "t3"),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.INT32, TSDataType.INT64),
            Arrays.asList(TSEncoding.RLE, TSEncoding.RLE, TSEncoding.RLBE),
            Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY, CompressionType.SNAPPY));
    template2.setId(2);
    ClusterTemplateManager.getInstance().putTemplate(template1);
    ClusterTemplateManager.getInstance().putTemplate(template2);
    clusterSchemaTree.appendTemplateDevice(new PartialPath("root.sg1.d1"), false, 1, template1);
    clusterSchemaTree.appendTemplateDevice(new PartialPath("root.sg1.d2"), false, 2, template2);
    clusterSchemaTree.setDatabases(Collections.singleton("root.sg1"));
    clusterSchemaTree.appendSingleMeasurementPath(
        new MeasurementPath("root.sg1.d3.s1", TSDataType.FLOAT));

    TreeDeviceSchemaCacheManager treeDeviceSchemaCacheManager =
        TreeDeviceSchemaCacheManager.getInstance();
    treeDeviceSchemaCacheManager.put(clusterSchemaTree);
    final ClusterSchemaTree d1Tree =
        treeDeviceSchemaCacheManager.getMatchedTemplateSchema(new PartialPath("root.sg1.d1"));

    final String[] ALL_RESULT_NODES = new String[] {"root", "sg1", "**"};
    final PartialPath ALL_MATCH_PATTERN = new PartialPath(ALL_RESULT_NODES);
    List<MeasurementPath> measurementPaths = d1Tree.searchMeasurementPaths(ALL_MATCH_PATTERN).left;
    return measurementPaths.get(0);
  }
}
