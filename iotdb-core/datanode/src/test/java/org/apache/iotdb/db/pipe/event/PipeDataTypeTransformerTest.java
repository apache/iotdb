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

package org.apache.iotdb.db.pipe.event;

import org.apache.iotdb.db.pipe.event.common.row.PipeDataTypeTransformer;
import org.apache.iotdb.pipe.api.type.Type;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class PipeDataTypeTransformerTest {
  private List<TSDataType> nullTsDataTypeList;
  private List<TSDataType> tsDataTypeList;
  private List<Type> nullTypeList;
  private List<Type> typeList;

  @Before
  public void setUp() {
    createDataTypeList();
  }

  public void createDataTypeList() {
    nullTsDataTypeList = null;
    tsDataTypeList = new ArrayList<>();
    tsDataTypeList.add(TSDataType.INT32);
    tsDataTypeList.add(TSDataType.INT64);
    tsDataTypeList.add(TSDataType.FLOAT);
    tsDataTypeList.add(TSDataType.DOUBLE);
    tsDataTypeList.add(TSDataType.BOOLEAN);
    tsDataTypeList.add(TSDataType.TEXT);
    tsDataTypeList.add(null);

    nullTypeList = null;
    typeList = new ArrayList<>();
    typeList.add(Type.INT32);
    typeList.add(Type.INT64);
    typeList.add(Type.FLOAT);
    typeList.add(Type.DOUBLE);
    typeList.add(Type.BOOLEAN);
    typeList.add(Type.TEXT);
    typeList.add(null);
  }

  @Test
  public void testDataTypeTransformer() {
    List<Type> nullResultList =
        PipeDataTypeTransformer.transformToPipeDataTypeList(nullTsDataTypeList);
    List<Type> resultList = PipeDataTypeTransformer.transformToPipeDataTypeList(tsDataTypeList);
    Assert.assertEquals(nullResultList, nullTypeList);
    Assert.assertEquals(resultList, typeList);
  }
}
