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

package org.apache.iotdb.cluster.query;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.cluster.common.EnvironmentUtils;
import org.apache.iotdb.cluster.common.TestManagedSeriesReader;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.executor.IQueryProcessExecutor;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.ManagedSeriesReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;

public class BaseQueryTest {

  MetaGroupMember metaGroupMember;
  List<Path> pathList;
  List<TSDataType> dataTypes;
  IQueryProcessExecutor queryProcessExecutor;

  @Before
  public void setUp() throws MetadataException {
    metaGroupMember = new TestMetaGroupMember() {
      @Override
      public TSDataType getSeriesType(String pathStr) {
        for (int i = 0; i < pathList.size(); i++) {
          if (pathList.get(i).getFullPath().equals(pathStr)) {
            return dataTypes.get(i);
          }
        }
        return null;
      }

      @Override
      public ManagedSeriesReader getSeriesReader(Path path, TSDataType dataType, Filter filter,
          QueryContext context, boolean pushDownUnseq, boolean withValueFilter) {
        int pathIndex = pathList.indexOf(path);
        if (pathIndex == -1) {
          return null;
        }
        return new TestManagedSeriesReader(TestUtils.genBatchData(dataTypes.get(pathIndex), 0,
            100), filter);
      }

      @Override
      public IReaderByTimestamp getReaderByTimestamp(Path path, QueryContext context) {
        for (int i = 0; i < pathList.size(); i++) {
          if (pathList.get(i).equals(path)) {
            return new TestManagedSeriesReader(TestUtils.genBatchData(dataTypes.get(i), 0,
                100), null);
          }
        }
        return null;
      }

      @Override
      public List<String> getMatchedPaths(String storageGroupName, String pathPattern)
          throws MetadataException {
        return MManager.getInstance().getPaths(pathPattern);
      }
    };

    pathList = new ArrayList<>();
    dataTypes = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      pathList.add(new Path(TestUtils.getTestSeries(0, i)));
      dataTypes.add(TSDataType.DOUBLE);
    }

    queryProcessExecutor = new QueryProcessExecutor() {
      @Override
      public List<String> getAllMatchedPaths(String originPath) throws MetadataException {
        return MManager.getInstance().getPaths(originPath);
      }

      @Override
      public TSDataType getSeriesType(Path path) {
        try {
          return metaGroupMember.getSeriesType(path.getFullPath());
        } catch (MetadataException e) {
          return null;
        }
      }
    };

    MManager.getInstance().init();
    for (int i = 0; i < 10; i++) {
      MManager.getInstance().setStorageGroupToMTree(TestUtils.getTestSg(i));
      MeasurementSchema schema = TestUtils.getTestSchema(0, i);
      MManager.getInstance().addPathToMTree(schema.getMeasurementId(), schema.getType(),
          schema.getEncodingType(), schema.getCompressor(), schema.getProps());
    }
  }

  @After
  public void tearDown() throws IOException {
    MManager.getInstance().clear();
    EnvironmentUtils.cleanAllDir();
  }

  void checkDataset(QueryDataSet dataSet, int offset, int size) throws IOException {
    for (int i = offset; i < offset + size; i++) {
      assertTrue(dataSet.hasNext());
      RowRecord record = dataSet.next();
      assertEquals(i, record.getTimestamp());
      assertEquals(10, record.getFields().size());
      for (int j = 0; j < 10; j++) {
        assertEquals(i * 1.0, record.getFields().get(j).getDoubleV(), 0.00001);
      }
    }
    assertFalse(dataSet.hasNext());
  }
}
