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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
  public void testAnalyzeVisitorReplaceAliasWithPhysicalPathKeepsQueryGeneratedState()
      throws Exception {
    PartialPath originalPath = new PartialPath("root.sg1.d3.temperature");
    MeasurementPath aliasPath =
        new MeasurementPath(
            new PartialPath("root.view.d3.temperature").getNodes(),
            createAliasSchema(originalPath, true));

    AnalyzeVisitor analyzeVisitor = new AnalyzeVisitor(null, null);
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
    schemaTree.appendSingleMeasurementPath(
        new MeasurementPath(
            physicalPath.getNodes(), createInvalidPhysicalSchema(physicalPath, aliasPath)));

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
    assertQueryGeneratedPhysicalSeriesProps(selectedPath, "root.view.d5.temperature");
    Assert.assertNotNull(operand.getViewPath());
    Assert.assertEquals("root.view.d5.temperature", operand.getViewPath().getFullPath());
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
    MeasurementSchema schema = new MeasurementSchema("temperature", TSDataType.FLOAT);
    Map<String, String> originalProps = new HashMap<>();
    originalProps.put("encoding_hint", "kept");
    MeasurementPropsUtils.setOriginalPathIsAligned(originalProps, isAligned);
    schema.setProps(MeasurementPropsUtils.buildAliasSeriesProps(originalProps, originalPath));
    return schema;
  }

  private MeasurementSchema createInvalidPhysicalSchema(
      PartialPath physicalPath, PartialPath aliasPath) {
    MeasurementSchema schema =
        new MeasurementSchema(physicalPath.getMeasurement(), TSDataType.FLOAT);
    Map<String, String> props = new HashMap<>();
    props.put("encoding_hint", "kept");
    MeasurementPropsUtils.setAliasPath(props, aliasPath);
    MeasurementPropsUtils.setInvalid(props, true);
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
}
