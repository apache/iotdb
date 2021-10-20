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

package org.apache.iotdb.db.qp.physical;

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.logical.sys.AlterTimeSeriesOperator.AlterType;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator.AuthorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan.Factory;
import org.apache.iotdb.db.qp.physical.sys.AlterTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateMultiTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DataAuthPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan.LoadConfigurationPlanType;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class PhysicalPlanSerializeTest {

  public ByteBuffer serializePlan(PhysicalPlan plan) {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    try {
      plan.serialize(dataOutputStream);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  public PhysicalPlan testTwoSerializeMethodAndDeserialize(PhysicalPlan plan)
      throws IOException, IllegalPathException {
    ByteBuffer byteBuffer1 = serializePlan(plan);
    ByteBuffer byteBuffer2 = ByteBuffer.allocate(byteBuffer1.limit());

    plan.serialize(byteBuffer2);
    byteBuffer2.flip();
    Assert.assertEquals(byteBuffer1, byteBuffer2);

    return Factory.create(byteBuffer1);
  }

  @Test
  public void showTimeSeriesPlanSerializeTest() throws IllegalPathException, IOException {
    ShowTimeSeriesPlan timeSeriesPlan =
        new ShowTimeSeriesPlan(new PartialPath("root.sg.d1.s1"), true, "unit", "10", 0, 0, false);
    ByteBuffer byteBuffer = serializePlan(timeSeriesPlan);
    PhysicalPlan result = Factory.create(byteBuffer);
    Assert.assertEquals("root.sg.d1.s1", ((ShowTimeSeriesPlan) result).getPath().getFullPath());
    Assert.assertEquals(true, ((ShowTimeSeriesPlan) result).isContains());
    Assert.assertEquals("unit", ((ShowTimeSeriesPlan) result).getKey());
    Assert.assertEquals("10", ((ShowTimeSeriesPlan) result).getValue());
    Assert.assertEquals(0, ((ShowTimeSeriesPlan) result).getLimit());
    Assert.assertEquals(0, ((ShowTimeSeriesPlan) result).getOffset());
    Assert.assertEquals(false, ((ShowTimeSeriesPlan) result).isOrderByHeat());
  }

  @Test
  public void setTTLPlanSerializeTest() throws IllegalPathException, IOException {
    SetTTLPlan setTTLPlan = new SetTTLPlan(new PartialPath("root.sg"), 1000000L);

    PhysicalPlan result = testTwoSerializeMethodAndDeserialize(setTTLPlan);

    Assert.assertEquals(OperatorType.TTL, result.getOperatorType());
    Assert.assertEquals("root.sg", ((SetTTLPlan) result).getStorageGroup().getFullPath());
    Assert.assertEquals(1000000L, ((SetTTLPlan) result).getDataTTL());
  }

  @Test
  public void setStorageGroupPlanTest() throws IllegalPathException, IOException {
    SetStorageGroupPlan setStorageGroupPlan = new SetStorageGroupPlan(new PartialPath("root.sg"));

    PhysicalPlan result = testTwoSerializeMethodAndDeserialize(setStorageGroupPlan);

    Assert.assertEquals(OperatorType.SET_STORAGE_GROUP, result.getOperatorType());
    Assert.assertEquals("root.sg", ((SetStorageGroupPlan) result).getPath().getFullPath());
    Assert.assertEquals(result, setStorageGroupPlan);
  }

  @Test
  public void deleteTimeSeriesPlanSerializeTest() throws IllegalPathException, IOException {
    DeleteTimeSeriesPlan deleteTimeSeriesPlan =
        new DeleteTimeSeriesPlan(Collections.singletonList(new PartialPath("root.sg.d1.s1")));

    PhysicalPlan result = testTwoSerializeMethodAndDeserialize(deleteTimeSeriesPlan);

    Assert.assertEquals(OperatorType.DELETE_TIMESERIES, result.getOperatorType());
    Assert.assertEquals("root.sg.d1.s1", result.getPaths().get(0).getFullPath());
  }

  @Test
  public void deleteStorageGroupPlanSerializeTest() throws IllegalPathException, IOException {
    DeleteStorageGroupPlan deleteStorageGroupPlan =
        new DeleteStorageGroupPlan(Collections.singletonList(new PartialPath("root.sg")));

    PhysicalPlan result = testTwoSerializeMethodAndDeserialize(deleteStorageGroupPlan);

    Assert.assertEquals(OperatorType.DELETE_STORAGE_GROUP, result.getOperatorType());
    Assert.assertEquals("root.sg", result.getPaths().get(0).getFullPath());
  }

  @Test
  public void dataAuthPlanSerializeTest() throws IOException, IllegalPathException {
    DataAuthPlan dataAuthPlan =
        new DataAuthPlan(OperatorType.GRANT_WATERMARK_EMBEDDING, Arrays.asList("user1", "user2"));

    PhysicalPlan result = testTwoSerializeMethodAndDeserialize(dataAuthPlan);

    Assert.assertEquals(Arrays.asList("user1", "user2"), ((DataAuthPlan) result).getUsers());
  }

  @Test
  public void createTimeSeriesPlanSerializeTest1() throws IOException, IllegalPathException {
    CreateTimeSeriesPlan createTimeSeriesPlan =
        new CreateTimeSeriesPlan(
            new PartialPath("root.sg.d1.s1"),
            TSDataType.DOUBLE,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            Collections.singletonMap("prop1", "propValue1"),
            Collections.singletonMap("tag1", "tagValue1"),
            Collections.singletonMap("attr1", "attrValue1"),
            "temperature");

    PhysicalPlan result = testTwoSerializeMethodAndDeserialize(createTimeSeriesPlan);

    Assert.assertEquals(OperatorType.CREATE_TIMESERIES, result.getOperatorType());
    Assert.assertEquals(createTimeSeriesPlan, result);
  }

  @Test
  public void createTimeSeriesPlanSerializeTest2() throws IOException, IllegalPathException {
    CreateTimeSeriesPlan createTimeSeriesPlan =
        new CreateTimeSeriesPlan(
            new PartialPath("root.sg.d1.s1"),
            TSDataType.DOUBLE,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null);

    PhysicalPlan result = testTwoSerializeMethodAndDeserialize(createTimeSeriesPlan);

    Assert.assertEquals(OperatorType.CREATE_TIMESERIES, result.getOperatorType());
    Assert.assertEquals(createTimeSeriesPlan, result);
  }

  @Test
  public void createAlignedTimeSeriesPlanSerializeTest() throws IOException, IllegalPathException {
    CreateAlignedTimeSeriesPlan createAlignedTimeSeriesPlan =
        new CreateAlignedTimeSeriesPlan(
            new PartialPath("root.sg.d1"),
            Arrays.asList("s1", "s2"),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.INT32),
            Arrays.asList(TSEncoding.RLE, TSEncoding.RLE),
            CompressionType.SNAPPY,
            null);

    PhysicalPlan result = testTwoSerializeMethodAndDeserialize(createAlignedTimeSeriesPlan);

    Assert.assertEquals(OperatorType.CREATE_ALIGNED_TIMESERIES, result.getOperatorType());
    Assert.assertEquals(createAlignedTimeSeriesPlan, result);
  }

  @Test
  public void createMuSerializeTest1() throws IOException, IllegalPathException {
    CreateMultiTimeSeriesPlan plan = new CreateMultiTimeSeriesPlan();
    plan.setPaths(
        Arrays.asList(new PartialPath("root.sg.d1.s1"), new PartialPath("root.sg.d1.s2")));
    plan.setDataTypes(Arrays.asList(TSDataType.DOUBLE, TSDataType.INT64));
    plan.setEncodings(Arrays.asList(TSEncoding.GORILLA, TSEncoding.GORILLA));
    plan.setCompressors(Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY));
    plan.setProps(
        Arrays.asList(
            Collections.singletonMap("prop1", "propValue1"),
            Collections.singletonMap("prop2", "propValue2")));
    plan.setTags(
        Arrays.asList(
            Collections.singletonMap("tag1", "tagValue1"),
            Collections.singletonMap("tag2", "tagValue2")));
    plan.setAttributes(
        Arrays.asList(
            Collections.singletonMap("attr1", "attrValue1"),
            Collections.singletonMap("attr2", "attrValue2")));
    plan.setAlias(Arrays.asList("temperature", "speed"));

    PhysicalPlan result = testTwoSerializeMethodAndDeserialize(plan);

    Assert.assertEquals(OperatorType.CREATE_MULTI_TIMESERIES, result.getOperatorType());
    Assert.assertEquals(plan, result);
  }

  @Test
  public void createMuSerializeTest2() throws IOException, IllegalPathException {
    CreateMultiTimeSeriesPlan plan = new CreateMultiTimeSeriesPlan();
    plan.setPaths(
        Arrays.asList(new PartialPath("root.sg.d1.s1"), new PartialPath("root.sg.d1.s2")));
    plan.setDataTypes(Arrays.asList(TSDataType.DOUBLE, TSDataType.INT64));
    plan.setEncodings(Arrays.asList(TSEncoding.GORILLA, TSEncoding.GORILLA));
    plan.setCompressors(Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY));
    plan.setProps(null);
    plan.setTags(null);
    plan.setAttributes(null);
    plan.setAlias(null);

    PhysicalPlan result = testTwoSerializeMethodAndDeserialize(plan);

    Assert.assertEquals(OperatorType.CREATE_MULTI_TIMESERIES, result.getOperatorType());
    Assert.assertEquals(plan, result);
  }

  @Test
  public void createMuSerializeTest3() throws IOException, IllegalPathException {
    // same as:
    // create timeseries root.sg.d1.s0 with datatype=DOUBLE, encoding=GORILLA, compression=SNAPPY
    // create aligned timeseries root.sg.d1.(s1 INT64, s2 DOUBLE, s3 INT64)
    // with encoding=(GORILLA, GORILLA, GORILLA), compression=SNAPPY
    CreateMultiTimeSeriesPlan plan = new CreateMultiTimeSeriesPlan();
    plan.setPaths(
        Arrays.asList(new PartialPath("root.sg.d1.s0"), new PartialPath("root.sg.d1.(s1,s2,s3)")));
    plan.setDataTypes(
        Arrays.asList(TSDataType.DOUBLE, TSDataType.INT64, TSDataType.DOUBLE, TSDataType.INT64));
    plan.setEncodings(
        Arrays.asList(
            TSEncoding.GORILLA, TSEncoding.GORILLA, TSEncoding.GORILLA, TSEncoding.GORILLA));
    plan.setCompressors(Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY));
    plan.setProps(null);
    plan.setTags(null);
    plan.setAttributes(null);
    plan.setAlias(null);

    PhysicalPlan result = testTwoSerializeMethodAndDeserialize(plan);

    Assert.assertEquals(OperatorType.CREATE_MULTI_TIMESERIES, result.getOperatorType());
    Assert.assertEquals(plan, result);
  }

  @Test
  public void AlterTimeSeriesPlanSerializeTest() throws IOException, IllegalPathException {
    AlterTimeSeriesPlan alterTimeSeriesPlan =
        new AlterTimeSeriesPlan(
            new PartialPath("root.sg.d1.s1"),
            AlterType.RENAME,
            Collections.singletonMap("root.sg.d1.s1", "root.sg.device1.temperature"),
            null,
            null,
            null);

    ByteBuffer byteBuffer = serializePlan(alterTimeSeriesPlan);
    PhysicalPlan result = Factory.create(byteBuffer);
    Assert.assertEquals(alterTimeSeriesPlan, result);
  }

  @Test
  public void loadConfigurationPlanSerializeTest()
      throws QueryProcessException, IOException, IllegalPathException {
    Properties[] properties = new Properties[2];
    properties[0] = new Properties();
    properties[0].setProperty("prop1", "value1");
    properties[1] = null;

    LoadConfigurationPlan loadConfigurationPlan =
        new LoadConfigurationPlan(LoadConfigurationPlanType.GLOBAL, properties);
    ByteBuffer byteBuffer = serializePlan(loadConfigurationPlan);
    PhysicalPlan result = Factory.create(byteBuffer);
    Assert.assertEquals(OperatorType.LOAD_CONFIGURATION, result.getOperatorType());
    Assert.assertEquals(
        LoadConfigurationPlanType.GLOBAL,
        ((LoadConfigurationPlan) result).getLoadConfigurationPlanType());
    Assert.assertEquals(properties[0], ((LoadConfigurationPlan) result).getIoTDBProperties());
    Assert.assertEquals(properties[1], ((LoadConfigurationPlan) result).getClusterProperties());
  }

  @Test
  public void authorPlanSerializeTest() throws IOException, AuthException, IllegalPathException {
    AuthorPlan authorPlan =
        new AuthorPlan(AuthorType.CREATE_ROLE, "root", "root", "root", "", null, null);

    PhysicalPlan result = testTwoSerializeMethodAndDeserialize(authorPlan);

    Assert.assertEquals(result, authorPlan);
  }
}
