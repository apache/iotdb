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

package org.timecho.iotdb.db.queryengine.plan.statement.crud;

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.utils.MeasurementPropsUtils;
import org.apache.iotdb.db.queryengine.common.schematree.MeasurementSchemaInfo;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TimechoLoadAliasSeriesInsertTabletTest {

  @Test
  public void testInvalidSeriesShouldBeRejectedByDefault() throws Exception {
    final InsertTabletStatement statement =
        newInsertTabletStatement("root.timecho.load.ut.d1", "s1");

    final MeasurementSchema invalidSchema = new MeasurementSchema("s1", TSDataType.INT32);
    final Map<String, String> props = new HashMap<>();
    MeasurementPropsUtils.setInvalid(props, true);
    invalidSchema.setProps(props);

    try {
      statement.validateMeasurementSchema(
          0, new MeasurementSchemaInfo("s1", invalidSchema, null, null, null));
      Assert.fail("Expected invalid series to be rejected.");
    } catch (final SemanticException e) {
      Assert.assertTrue(e.getMessage().contains("Cannot insert data into invalid series"));
    }
  }

  @Test
  public void testInvalidPhysicalSeriesShouldBeAcceptedWhenExplicitlyAllowed() throws Exception {
    final InsertTabletStatement statement =
        newInsertTabletStatement("root.timecho.load.ut.d1", "s1");
    statement.setAllowInsertIntoInvalidSeries(true);

    final MeasurementSchema invalidSchema = new MeasurementSchema("s1", TSDataType.INT32);
    final Map<String, String> props = new HashMap<>();
    MeasurementPropsUtils.setInvalid(props, true);
    invalidSchema.setProps(props);

    statement.validateMeasurementSchema(
        0, new MeasurementSchemaInfo("s1", invalidSchema, null, null, null));

    Assert.assertNotNull(statement.getMeasurementSchemas());
    Assert.assertEquals("s1", statement.getMeasurementSchemas()[0].getMeasurementName());
    Assert.assertEquals(TSDataType.INT32, statement.getMeasurementSchemas()[0].getType());
  }

  @Test
  public void testRenamedSeriesShouldStillRecordOriginalPhysicalPath() throws Exception {
    final InsertTabletStatement statement =
        newInsertTabletStatement("root.timecho.load.ut.alias.d1", "s1_alias");
    statement.setAllowInsertIntoInvalidSeries(true);

    final MeasurementSchema aliasSchema = new MeasurementSchema("s1_alias", TSDataType.INT32);
    aliasSchema.setProps(
        MeasurementPropsUtils.buildAliasSeriesProps(
            Collections.emptyMap(), new PartialPath("root.timecho.load.ut.src.d1.s1")));

    statement.validateMeasurementSchema(
        0, new MeasurementSchemaInfo("s1_alias", aliasSchema, null, null, null));

    Assert.assertNotNull(statement.getAliasSeriesOriginalPathList());
    Assert.assertEquals(1, statement.getAliasSeriesOriginalPathList().size());
    Assert.assertEquals(
        "root.timecho.load.ut.src.d1.s1",
        statement.getAliasSeriesOriginalPathList().get(0).getFullPath());
    Assert.assertEquals(Integer.valueOf(0), statement.getIndexListOfAliasSeriesPaths().get(0));
  }

  @Test
  public void testSplitListUsesPhysicalDeviceAlignmentForAliasRedirect() throws Exception {
    final InsertTabletStatement statement =
        newInsertTabletStatement("root.timecho.load.ut.view.d1", "s1", "s2");
    statement.setAligned(false);
    statement.setTimes(new long[] {1L});
    statement.setColumns(new Object[] {new int[] {1}, new int[] {2}});
    statement.setRowCount(1);

    final MeasurementSchema aliasSchema = new MeasurementSchema("s1", TSDataType.INT32);
    aliasSchema.setProps(
        MeasurementPropsUtils.buildAliasSeriesProps(
            Collections.emptyMap(), new PartialPath("root.timecho.load.ut.src.d1.s1")));
    statement.validateMeasurementSchema(
        0, new MeasurementSchemaInfo("s1", aliasSchema, null, null, null));

    final MeasurementSchema aliasSchema2 = new MeasurementSchema("s2", TSDataType.INT32);
    aliasSchema2.setProps(
        MeasurementPropsUtils.buildAliasSeriesProps(
            Collections.emptyMap(), new PartialPath("root.timecho.load.ut.src.d1.s2")));
    statement.validateMeasurementSchema(
        1, new MeasurementSchemaInfo("s2", aliasSchema2, null, null, null));

    statement.computeMeasurementOfAliasSeries(
        0, new MeasurementSchemaInfo("s1", aliasSchema, null, null, null), true);
    statement.computeMeasurementOfAliasSeries(
        1, new MeasurementSchemaInfo("s2", aliasSchema2, null, null, null), true);

    Assert.assertTrue(statement.isAligned());
    final List<InsertTabletStatement> splitList = statement.getSplitList();
    Assert.assertEquals(1, splitList.size());
    Assert.assertEquals(
        "root.timecho.load.ut.src.d1", splitList.get(0).getDevicePath().getFullPath());
    Assert.assertTrue(splitList.get(0).isAligned());
  }

  @Test
  public void testSplitListKeepsStatementDeviceAlignmentWithoutAliasRedirect() throws Exception {
    final InsertTabletStatement statement =
        newInsertTabletStatement("root.timecho.load.ut.src.d1", "s1", "s2");
    statement.setAligned(true);
    statement.setTimes(new long[] {1L});
    statement.setColumns(new Object[] {new int[] {1}, new int[] {2}});
    statement.setRowCount(1);
    statement.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.INT32),
          new MeasurementSchema("s2", TSDataType.INT32)
        });

    final List<InsertTabletStatement> splitList = statement.getSplitList();
    Assert.assertEquals(1, splitList.size());
    Assert.assertTrue(splitList.get(0).isAligned());
  }

  private static InsertTabletStatement newInsertTabletStatement(
      final String device, final String measurement) throws Exception {
    final InsertTabletStatement statement = new InsertTabletStatement();
    statement.setDevicePath(new PartialPath(device));
    statement.setMeasurements(new String[] {measurement});
    statement.setDataTypes(new TSDataType[] {TSDataType.INT32});
    statement.setAligned(false);
    return statement;
  }

  private static InsertTabletStatement newInsertTabletStatement(
      final String device, final String... measurements) throws Exception {
    final InsertTabletStatement statement = new InsertTabletStatement();
    statement.setDevicePath(new PartialPath(device));
    statement.setMeasurements(measurements);
    statement.setDataTypes(new TSDataType[measurements.length]);
    for (int i = 0; i < measurements.length; i++) {
      statement.getDataTypes()[i] = TSDataType.INT32;
    }
    statement.setAligned(false);
    return statement;
  }
}
