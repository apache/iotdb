/**
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

package org.apache.iotdb.db.qp.plan.logicalPlan;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.RootOperator;
import org.apache.iotdb.db.qp.logical.sys.MetadataOperator;
import org.apache.iotdb.db.qp.logical.sys.PropertyOperator;
import org.apache.iotdb.db.qp.strategy.LogicalGenerator;
import org.apache.iotdb.db.sql.parse.SqlParseException;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestMetadataStatement {
  private LogicalGenerator generator;

  @Before
  public void before() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    generator = new LogicalGenerator(config.getZoneID());
  }

  @Test
  public void createTimeseries() {
    RootOperator op = generator.getLogicalPlan(
            "create timeseries root.laptop.d0.s2 with datatype=FLOAT"
                    + ",encoding=RLE,freq_encoding=DFT,write_main_freq=true,dft_pack_length=300,dft_rate=0.4,write_encoding=False;");
    assertEquals(SQLConstant.TOK_METADATA_CREATE, op.getTokenIntType());
    assertEquals(new Path("root.laptop.d0.s2"), ((MetadataOperator)op).getPath());
    assertEquals(TSDataType.FLOAT, ((MetadataOperator)op).getDataType());
    assertEquals(TSEncoding.RLE, ((MetadataOperator)op).getEncoding());
    assertEquals(TSFileConfig.compressor, ((MetadataOperator)op).getCompressor().toString());
    Map<String, String> prop = ((MetadataOperator)op).getProps();
    assertEquals(5, prop.size());
    assertEquals("DFT", prop.get("freq_encoding"));
    assertEquals("true", prop.get("write_main_freq"));
    assertEquals("300", prop.get("dft_pack_length"));
    assertEquals("0.4", prop.get("dft_rate"));
    assertEquals("False", prop.get("write_encoding"));
  }

  @Test (expected = SqlParseException.class)
  public void createTimeseries2() {
    RootOperator op = generator.getLogicalPlan(
            "create timeseries root.laptop.d1.s1 with datatype=INT64,encoding=rle");

  }

  @Test
  public void setStorageGroup1() {
    RootOperator op = generator.getLogicalPlan("set storage group to root.a.b.c");

  }

  @Test
  public void setStorageGroup2() {
    MetadataOperator op = (MetadataOperator) generator.getLogicalPlan(
            "set storage group to root.a.b.c;");
    assertEquals(SQLConstant.TOK_METADATA_SET_FILE_LEVEL, op.getTokenIntType());
    assertEquals(MetadataOperator.NamespaceType.SET_STORAGE_GROUP, op.getNamespaceType());
    assertEquals(new Path("root.a.b.c"), op.getPath());
  }

  @Test
  public void createProp() {
    RootOperator op = generator.getLogicalPlan("create property myprop");
    assertEquals(SQLConstant.TOK_PROPERTY_CREATE, op.getTokenIntType());
    assertEquals(PropertyOperator.PropertyType.ADD_TREE, ((PropertyOperator)op).getPropertyType());
    assertEquals(new Path("myprop"), ((PropertyOperator)op).getPropertyPath());
  }

  @Test
  public void addLabelToProp() {
    RootOperator op = generator.getLogicalPlan("ADD LABEL myLabel TO PROPERTY myProp;");
    assertEquals(SQLConstant.TOK_PROPERTY_ADD_LABEL, op.getTokenIntType());
    assertEquals(PropertyOperator.PropertyType.ADD_PROPERTY_LABEL, ((PropertyOperator)op).getPropertyType());
    assertEquals(new Path(new String[]{"myProp","myLabel"}), ((PropertyOperator)op).getPropertyPath());
  }

  @Test
  public void deleteLabelFromProp() {
    RootOperator op = generator.getLogicalPlan("DELETE LABEL myLable FROM PROPERTY myProp");
    String[] s = new String[]{"myProp", "myLable"};
    assertEquals(new Path(s), ((PropertyOperator)op).getPropertyPath());
  }


  @Test
  public void linkLabel() {
    RootOperator op = generator.getLogicalPlan("link root.a.b.c to myProp.myLabel;");
    assertEquals(SQLConstant.TOK_PROPERTY_LINK, op.getTokenIntType());
    assertEquals(PropertyOperator.PropertyType.ADD_PROPERTY_TO_METADATA, ((PropertyOperator)op).getPropertyType());
    assertEquals(new Path("root.a.b.c"), ((PropertyOperator)op).getMetadataPath());
    assertEquals(new Path("myProp.myLabel"), ((PropertyOperator)op).getPropertyPath());
  }

  @Test
  public void unlinkLabel() {
    RootOperator op = generator.getLogicalPlan(
            "unlink root.m1.m2 from myProp.myLabel");
    assertEquals(SQLConstant.TOK_PROPERTY_UNLINK, op.getTokenIntType());
    assertEquals(PropertyOperator.PropertyType.DEL_PROPERTY_FROM_METADATA, ((PropertyOperator)op).getPropertyType());
    assertEquals(new Path("root.m1.m2"), ((PropertyOperator)op).getMetadataPath());
    assertEquals(new Path("myProp.myLabel"), ((PropertyOperator)op).getPropertyPath());
    
  }

  @Test
  public void deleteTimeseries1() {
    RootOperator op = generator.getLogicalPlan("delete timeseries root.dt.a.b.d1.s1;");
    assertEquals(SQLConstant.TOK_METADATA_DELETE, op.getTokenIntType());
    assertEquals(MetadataOperator.NamespaceType.DELETE_PATH, ((MetadataOperator)op).getNamespaceType());
    List<Path> deletePaths = ((MetadataOperator)op).getDeletePathList();
    assertEquals(1, deletePaths.size());
    assertEquals(new Path("root.dt.a.b.d1.s1"), deletePaths.get(0));
  }

  @Test
  public void deleteTimeseries2() {
    RootOperator op = generator.getLogicalPlan("delete timeseries root.*");
    assertEquals(SQLConstant.TOK_METADATA_DELETE, op.getTokenIntType());
    assertEquals(MetadataOperator.NamespaceType.DELETE_PATH, ((MetadataOperator)op).getNamespaceType());
    List<Path> deletePaths = ((MetadataOperator)op).getDeletePathList();
    assertEquals(1, deletePaths.size());
    assertEquals(new Path("root.*"), deletePaths.get(0));

  }

  @Test
  public void deleteTimeseries3() {
    RootOperator op = generator.getLogicalPlan("delete timeseries root.dt.a.b,root.*;");
    assertEquals(SQLConstant.TOK_METADATA_DELETE, op.getTokenIntType());
    assertEquals(MetadataOperator.NamespaceType.DELETE_PATH, ((MetadataOperator)op).getNamespaceType());
    List<Path> deletePaths = ((MetadataOperator)op).getDeletePathList();
    assertEquals(2, deletePaths.size());
    assertEquals(new Path("root.dt.a.b"), deletePaths.get(0));
    assertEquals(new Path("root.*"), deletePaths.get(1));
  }

  @Test (expected = SqlParseException.class)
  public void showMetadata() {
    RootOperator op = generator.getLogicalPlan("show metadata;");
  }

  @Test (expected = SqlParseException.class)
  public void describePath() {
    RootOperator op = generator.getLogicalPlan("describe root");

  }
}
