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

package org.apache.iotdb.db.queryengine.plan;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.utils.MeasurementPropsUtils;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.common.schematree.MeasurementSchemaInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor;
import org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.gt;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.intValue;

public class AliasSeriesPhysicalSchemaTest {

  @Test
  public void testInsertRowAliasSeriesKeepsQueryGeneratedPhysicalSeriesState() throws Exception {
    InsertRowStatement statement = new InsertRowStatement();
    statement.setDevicePath(new PartialPath("root.view.d1"));
    statement.setMeasurements(new String[] {"temperature"});

    MeasurementPath physicalPath =
        validateInsertAndGetPhysicalPath(
            statement,
            new PartialPath("root.sg1.d1.temperature"),
            new PartialPath("root.view.d1.temperature"),
            true);

    assertQueryGeneratedPhysicalSeriesProps(physicalPath, "root.view.d1.temperature");
    Assert.assertTrue(physicalPath.isUnderAlignedEntity());
  }

  @Test
  public void testInsertTabletAliasSeriesKeepsQueryGeneratedPhysicalSeriesState() throws Exception {
    InsertTabletStatement statement = new InsertTabletStatement();
    statement.setDevicePath(new PartialPath("root.view.d2"));
    statement.setMeasurements(new String[] {"temperature"});

    MeasurementPath physicalPath =
        validateInsertAndGetPhysicalPath(
            statement,
            new PartialPath("root.sg1.d2.temperature"),
            new PartialPath("root.view.d2.temperature"),
            false);

    assertQueryGeneratedPhysicalSeriesProps(physicalPath, "root.view.d2.temperature");
    Assert.assertFalse(physicalPath.isUnderAlignedEntity());
  }

  @Test
  public void testSplitInsertRowKeepsNonAliasDeviceNonAlignedWhenAliasSeriesIsAligned()
      throws Exception {
    InsertRowStatement statement = new InsertRowStatement();
    statement.setDevicePath(new PartialPath("root.view.d1"));
    statement.setMeasurements(new String[] {"s1", "s2", "s3"});
    statement.setDataTypes(new TSDataType[] {TSDataType.INT32, TSDataType.TEXT, TSDataType.FLOAT});
    statement.setValues(new Object[] {1, "a", 1.1f});
    statement.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.INT32),
          new MeasurementSchema("s2", TSDataType.TEXT),
          new MeasurementSchema("s3", TSDataType.FLOAT)
        });
    statement.setAligned(false);

    MeasurementSchemaInfo s1AliasInfo =
        new MeasurementSchemaInfo(
            "s1",
            createAliasSchema("s1", TSDataType.INT32, new PartialPath("root.src.d1.s1")),
            null,
            null,
            null);
    MeasurementSchemaInfo s2AliasInfo =
        new MeasurementSchemaInfo(
            "s2",
            createAliasSchema("s2", TSDataType.TEXT, new PartialPath("root.src.d1.s2")),
            null,
            null,
            null);
    statement.validateMeasurementSchema(0, s1AliasInfo);
    statement.validateMeasurementSchema(1, s2AliasInfo);
    statement.computeMeasurementOfAliasSeries(0, s1AliasInfo, true);
    statement.computeMeasurementOfAliasSeries(1, s2AliasInfo, true);
    statement.validateMeasurementSchema(
        2,
        new MeasurementSchemaInfo(
            "s3", new MeasurementSchema("s3", TSDataType.FLOAT), null, null, null));

    Assert.assertTrue(statement.isAligned());

    List<InsertRowStatement> splitList = statement.getSplitList();
    Assert.assertEquals(2, splitList.size());
    Assert.assertTrue(findRowSplit(splitList, "root.src.d1").isAligned());
    Assert.assertFalse(findRowSplit(splitList, "root.view.d1").isAligned());
  }

  @Test
  public void testSplitInsertTabletKeepsNonAliasDeviceNonAlignedWhenAliasSeriesIsAligned()
      throws Exception {
    InsertTabletStatement statement = new InsertTabletStatement();
    statement.setDevicePath(new PartialPath("root.view.d1"));
    statement.setMeasurements(new String[] {"s1", "s2", "s3"});
    statement.setDataTypes(new TSDataType[] {TSDataType.INT32, TSDataType.TEXT, TSDataType.FLOAT});
    statement.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.INT32),
          new MeasurementSchema("s2", TSDataType.TEXT),
          new MeasurementSchema("s3", TSDataType.FLOAT)
        });
    statement.setTimes(new long[] {1L});
    statement.setColumns(
        new Object[] {
          new int[] {1},
          new Binary[] {new Binary("a", TSFileConfig.STRING_CHARSET)},
          new float[] {1.1f}
        });
    statement.setRowCount(1);
    statement.setAligned(false);

    MeasurementSchemaInfo s1AliasInfo =
        new MeasurementSchemaInfo(
            "s1",
            createAliasSchema("s1", TSDataType.INT32, new PartialPath("root.src.d1.s1")),
            null,
            null,
            null);
    MeasurementSchemaInfo s2AliasInfo =
        new MeasurementSchemaInfo(
            "s2",
            createAliasSchema("s2", TSDataType.TEXT, new PartialPath("root.src.d1.s2")),
            null,
            null,
            null);
    statement.validateMeasurementSchema(0, s1AliasInfo);
    statement.validateMeasurementSchema(1, s2AliasInfo);
    statement.computeMeasurementOfAliasSeries(0, s1AliasInfo, true);
    statement.computeMeasurementOfAliasSeries(1, s2AliasInfo, true);
    statement.validateMeasurementSchema(
        2,
        new MeasurementSchemaInfo(
            "s3", new MeasurementSchema("s3", TSDataType.FLOAT), null, null, null));

    Assert.assertTrue(statement.isAligned());

    List<InsertTabletStatement> splitList = statement.getSplitList();
    Assert.assertEquals(2, splitList.size());
    Assert.assertTrue(findTabletSplit(splitList, "root.src.d1").isAligned());
    Assert.assertFalse(findTabletSplit(splitList, "root.view.d1").isAligned());
  }

  @Test
  public void testAnalyzeVisitorReplaceAliasWithPhysicalPathKeepsQueryGeneratedState()
      throws Exception {
    PartialPath originalPath = new PartialPath("root.sg1.d3.temperature");
    MeasurementPath aliasPath =
        new MeasurementPath(
            new PartialPath("root.view.d3.temperature").getNodes(),
            createAliasSchema(originalPath, true));

    AnalyzeVisitor analyzeVisitor = new AnalyzeVisitor(null, null, null);
    Method replacePathWithOriginal =
        AnalyzeVisitor.class.getDeclaredMethod(
            "replacePathWithOriginal", PartialPath.class, MeasurementPath.class);
    replacePathWithOriginal.setAccessible(true);

    MeasurementPath replacedPath =
        (MeasurementPath) replacePathWithOriginal.invoke(analyzeVisitor, originalPath, aliasPath);

    Assert.assertEquals("root.sg1.d3.temperature", replacedPath.getFullPath());
    Assert.assertTrue(replacedPath.isUnderAlignedEntity());
    assertQueryGeneratedPhysicalSeriesProps(replacedPath, "root.view.d3.temperature");
  }

  @Test
  public void testBindSchemaForExpressionAliasSeriesUsesQueryGeneratedPhysicalPath()
      throws Exception {
    PartialPath physicalPath = new PartialPath("root.sg1.d5.temperature");
    PartialPath aliasPath = new PartialPath("root.view.d5.temperature");

    ClusterSchemaTree schemaTree = new ClusterSchemaTree();
    schemaTree.appendSingleMeasurementPath(
        new MeasurementPath(aliasPath.getNodes(), createAliasSchema(physicalPath, false)));
    MeasurementPath physicalMeasurementPath =
        new MeasurementPath(
            physicalPath.getNodes(), createInvalidPhysicalSchema(physicalPath, aliasPath));
    physicalMeasurementPath.setUnderAlignedEntity(true);
    schemaTree.appendSingleMeasurementPath(physicalMeasurementPath);

    List<Expression> boundExpressions =
        ExpressionAnalyzer.bindSchemaForExpression(
            new TimeSeriesOperand(aliasPath),
            schemaTree,
            new MPPQueryContext(new QueryId("test_query_generated_alias_binding")));

    Assert.assertEquals(1, boundExpressions.size());
    Assert.assertTrue(boundExpressions.get(0) instanceof TimeSeriesOperand);

    TimeSeriesOperand operand = (TimeSeriesOperand) boundExpressions.get(0);
    MeasurementPath selectedPath = (MeasurementPath) operand.getPath();
    Assert.assertEquals("root.sg1.d5.temperature", selectedPath.getFullPath());
    Assert.assertTrue(selectedPath.isUnderAlignedEntity());
    Assert.assertEquals(TSDataType.FLOAT, selectedPath.getMeasurementSchema().getType());
    assertQueryGeneratedPhysicalSeriesProps(selectedPath, "root.view.d5.temperature");
    Assert.assertNotNull(operand.getViewPath());
    Assert.assertEquals("root.view.d5.temperature", operand.getViewPath().getFullPath());
  }

  @Test
  public void testBindSchemaForExpressionWildcardSkipsQueryGeneratedPhysicalPath()
      throws Exception {
    PartialPath physicalPath = new PartialPath("root.sg1.d7.temperature");
    PartialPath aliasPath = new PartialPath("root.view.d7.temperature");

    ClusterSchemaTree schemaTree = new ClusterSchemaTree();
    MeasurementPath queryGeneratedPhysicalPath =
        new MeasurementPath(
            physicalPath.getNodes(), createQueryGeneratedPhysicalSchema(physicalPath, aliasPath));
    queryGeneratedPhysicalPath.setUnderAlignedEntity(false);
    schemaTree.appendSingleMeasurementPath(queryGeneratedPhysicalPath);
    schemaTree.appendSingleMeasurementPath(
        new MeasurementPath(aliasPath.getNodes(), createAliasSchema(physicalPath, true)));

    List<Expression> boundExpressions =
        ExpressionAnalyzer.bindSchemaForExpression(
            new TimeSeriesOperand(new PartialPath("root.**")),
            schemaTree,
            new MPPQueryContext(new QueryId("test_query_generated_alias_wildcard_binding")));

    Assert.assertEquals(1, boundExpressions.size());
    Assert.assertTrue(boundExpressions.get(0) instanceof TimeSeriesOperand);

    TimeSeriesOperand operand = (TimeSeriesOperand) boundExpressions.get(0);
    MeasurementPath selectedPath = (MeasurementPath) operand.getPath();
    Assert.assertEquals("root.sg1.d7.temperature", selectedPath.getFullPath());
    Assert.assertTrue(selectedPath.isUnderAlignedEntity());
    assertQueryGeneratedPhysicalSeriesProps(selectedPath, "root.view.d7.temperature");
    Assert.assertNotNull(operand.getViewPath());
    Assert.assertEquals("root.view.d7.temperature", operand.getViewPath().getFullPath());
  }

  @Test
  public void testBindSchemaForPredicateWildcardSkipsQueryGeneratedPhysicalPath() throws Exception {
    PartialPath physicalPath = new PartialPath("root.sg1.d8.temperature");
    PartialPath aliasPath = new PartialPath("root.view.d8.temperature");

    ClusterSchemaTree schemaTree = new ClusterSchemaTree();
    MeasurementPath queryGeneratedPhysicalPath =
        new MeasurementPath(
            physicalPath.getNodes(), createQueryGeneratedPhysicalSchema(physicalPath, aliasPath));
    queryGeneratedPhysicalPath.setUnderAlignedEntity(false);
    schemaTree.appendSingleMeasurementPath(queryGeneratedPhysicalPath);
    schemaTree.appendSingleMeasurementPath(
        new MeasurementPath(aliasPath.getNodes(), createAliasSchema(physicalPath, true)));

    List<Expression> boundPredicates =
        ExpressionAnalyzer.bindSchemaForPredicate(
            gt(new TimeSeriesOperand(new PartialPath("root.**")), intValue("1")),
            Collections.emptyList(),
            schemaTree,
            true,
            new MPPQueryContext(new QueryId("test_query_generated_alias_predicate_binding")));

    Assert.assertEquals(1, boundPredicates.size());

    Expression boundPredicate = boundPredicates.get(0);
    Assert.assertEquals(2, boundPredicate.getExpressions().size());
    Assert.assertTrue(boundPredicate.getExpressions().get(0) instanceof TimeSeriesOperand);

    TimeSeriesOperand operand = (TimeSeriesOperand) boundPredicate.getExpressions().get(0);
    MeasurementPath selectedPath = (MeasurementPath) operand.getPath();
    Assert.assertEquals("root.sg1.d8.temperature", selectedPath.getFullPath());
    Assert.assertTrue(selectedPath.isUnderAlignedEntity());
    assertQueryGeneratedPhysicalSeriesProps(selectedPath, "root.view.d8.temperature");
    Assert.assertNotNull(operand.getViewPath());
    Assert.assertEquals("root.view.d8.temperature", operand.getViewPath().getFullPath());
  }

  @Test
  public void testQueryGeneratedPhysicalSeriesPropsWrapperMasksAliasInternalKeys()
      throws Exception {
    final PartialPath physicalPath = new PartialPath("root.sg1.d6.temperature");
    final PartialPath aliasPath = new PartialPath("root.view.d6.temperature");
    final MeasurementSchema aliasSchema = createAliasSchema(physicalPath, false);
    final Map<String, String> queryGeneratedProps =
        MeasurementPropsUtils.buildQueryGeneratedPhysicalSeriesProps(
            aliasSchema.getProps(), aliasPath);

    Assert.assertEquals("kept", queryGeneratedProps.get("encoding_hint"));
    Assert.assertNull(queryGeneratedProps.get(MeasurementPropsUtils.IS_RENAMED_KEY));
    Assert.assertNull(queryGeneratedProps.get(MeasurementPropsUtils.IS_RENAMING_KEY));
    Assert.assertNull(queryGeneratedProps.get(MeasurementPropsUtils.INVALID_KEY));
    Assert.assertNull(queryGeneratedProps.get(MeasurementPropsUtils.ORIGINAL_PATH_KEY));
    Assert.assertNull(queryGeneratedProps.get(MeasurementPropsUtils.ORIGINAL_PATH_IS_ALIGNED_KEY));
    Assert.assertEquals(
        aliasPath.getFullPath(), queryGeneratedProps.get(MeasurementPropsUtils.ALIAS_PATH_KEY));
    Assert.assertFalse(queryGeneratedProps.containsKey(MeasurementPropsUtils.IS_RENAMED_KEY));
    Assert.assertFalse(MeasurementPropsUtils.isInvalid(queryGeneratedProps));
    Assert.assertFalse(MeasurementPropsUtils.isRenamed(queryGeneratedProps));
    Assert.assertFalse(MeasurementPropsUtils.isRenaming(queryGeneratedProps));
    Assert.assertNull(MeasurementPropsUtils.getOriginalPath(queryGeneratedProps));
    Assert.assertFalse(
        queryGeneratedProps.containsKey(MeasurementPropsUtils.ORIGINAL_PATH_IS_ALIGNED_KEY));
    Assert.assertFalse(queryGeneratedProps.containsKey(MeasurementPropsUtils.INVALID_KEY));
    Assert.assertFalse(queryGeneratedProps.containsKey(MeasurementPropsUtils.IS_RENAMING_KEY));
    Assert.assertFalse(queryGeneratedProps.containsKey(MeasurementPropsUtils.ORIGINAL_PATH_KEY));
    Assert.assertTrue(queryGeneratedProps.containsKey(MeasurementPropsUtils.ALIAS_PATH_KEY));
    Assert.assertEquals(
        aliasPath.getFullPath(), MeasurementPropsUtils.getAliasPathString(queryGeneratedProps));
    Assert.assertTrue(MeasurementPropsUtils.isQueryGeneratedInvalidSeries(queryGeneratedProps));

    final Set<Map.Entry<String, String>> entrySetView = queryGeneratedProps.entrySet();
    Assert.assertTrue(
        entrySetView.contains(new AbstractMap.SimpleImmutableEntry<>("encoding_hint", "kept")));
    Assert.assertTrue(
        entrySetView.contains(
            new AbstractMap.SimpleImmutableEntry<>(
                MeasurementPropsUtils.ALIAS_PATH_KEY, aliasPath.getFullPath())));
    Assert.assertFalse(
        entrySetView.contains(
            new AbstractMap.SimpleImmutableEntry<>(MeasurementPropsUtils.INVALID_KEY, "true")));
  }

  private MeasurementPath validateInsertAndGetPhysicalPath(
      Object statement, PartialPath originalPath, PartialPath aliasPath, boolean isAligned)
      throws Exception {
    MeasurementSchema aliasSchema = createAliasSchema(originalPath, isAligned);
    MeasurementSchemaInfo schemaInfo =
        new MeasurementSchemaInfo(aliasPath.getMeasurement(), aliasSchema, null, null, null);

    if (statement instanceof InsertRowStatement) {
      ((InsertRowStatement) statement).validateMeasurementSchema(0, schemaInfo);
      Assert.assertEquals(
          1, ((InsertRowStatement) statement).getAliasSeriesOriginalPathList().size());
      return (MeasurementPath)
          ((InsertRowStatement) statement).getAliasSeriesOriginalPathList().get(0);
    }

    ((InsertTabletStatement) statement).validateMeasurementSchema(0, schemaInfo);
    Assert.assertEquals(
        1, ((InsertTabletStatement) statement).getAliasSeriesOriginalPathList().size());
    return (MeasurementPath)
        ((InsertTabletStatement) statement).getAliasSeriesOriginalPathList().get(0);
  }

  private MeasurementSchema createAliasSchema(PartialPath originalPath, boolean isAligned) {
    MeasurementSchema schema = createAliasSchema("temperature", TSDataType.FLOAT, originalPath);
    Map<String, String> originalProps = new HashMap<>();
    originalProps.put("encoding_hint", "kept");
    MeasurementPropsUtils.setOriginalPathIsAligned(originalProps, isAligned);
    schema.setProps(MeasurementPropsUtils.buildAliasSeriesProps(originalProps, originalPath));
    return schema;
  }

  private MeasurementSchema createAliasSchema(
      String measurement, TSDataType type, PartialPath originalPath) {
    MeasurementSchema schema = new MeasurementSchema(measurement, type);
    Map<String, String> originalProps = new HashMap<>();
    MeasurementPropsUtils.setOriginalPathIsAligned(originalProps, true);
    schema.setProps(MeasurementPropsUtils.buildAliasSeriesProps(originalProps, originalPath));
    return schema;
  }

  private MeasurementSchema createInvalidPhysicalSchema(
      PartialPath physicalPath, PartialPath aliasPath) {
    MeasurementSchema schema =
        new MeasurementSchema(physicalPath.getMeasurement(), TSDataType.TEXT);
    Map<String, String> props = new HashMap<>();
    props.put("encoding_hint", "kept");
    MeasurementPropsUtils.setAliasPath(props, aliasPath);
    MeasurementPropsUtils.setInvalid(props, true);
    schema.setProps(props);
    return schema;
  }

  private MeasurementSchema createQueryGeneratedPhysicalSchema(
      PartialPath physicalPath, PartialPath aliasPath) {
    MeasurementSchema schema =
        new MeasurementSchema(physicalPath.getMeasurement(), TSDataType.FLOAT);
    Map<String, String> props = new HashMap<>();
    props.put("encoding_hint", "kept");
    MeasurementPropsUtils.setAliasPath(props, aliasPath);
    MeasurementPropsUtils.setQueryGeneratedInvalidSeries(props, true);
    schema.setProps(props);
    return schema;
  }

  private void assertQueryGeneratedPhysicalSeriesProps(MeasurementPath path, String aliasPath) {
    Map<String, String> props = path.getMeasurementSchema().getProps();
    Assert.assertNotNull(props);
    Assert.assertEquals("kept", props.get("encoding_hint"));
    Assert.assertFalse(MeasurementPropsUtils.isInvalid(props));
    Assert.assertFalse(MeasurementPropsUtils.isRenamed(props));
    Assert.assertNull(MeasurementPropsUtils.getOriginalPath(props));
    Assert.assertEquals(aliasPath, MeasurementPropsUtils.getAliasPathString(props));
    Assert.assertTrue(MeasurementPropsUtils.isQueryGeneratedInvalidSeries(props));
  }

  private InsertRowStatement findRowSplit(List<InsertRowStatement> splitList, String devicePath) {
    for (InsertRowStatement statement : splitList) {
      if (devicePath.equals(statement.getDevicePath().getFullPath())) {
        return statement;
      }
    }
    Assert.fail("Missing split statement for device " + devicePath);
    return null;
  }

  private InsertTabletStatement findTabletSplit(
      List<InsertTabletStatement> splitList, String devicePath) {
    for (InsertTabletStatement statement : splitList) {
      if (devicePath.equals(statement.getDevicePath().getFullPath())) {
        return statement;
      }
    }
    Assert.fail("Missing split statement for device " + devicePath);
    return null;
  }
}
