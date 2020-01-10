/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.query.reader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.iotdb.cluster.common.TestManagedSeriesReader;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.ManagedSeriesReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.junit.Test;

public class ClusterTimeGeneratorTest {

  private MetaGroupMember metaGroupMember = new TestMetaGroupMember() {
    @Override
    public ManagedSeriesReader getSeriesReader(Path path, TSDataType dataType, Filter filter,
        QueryContext context, boolean pushDownUnseq, boolean withValueFilter) {
      BatchData batchData = TestUtils.genBatchData(dataType, 0, 100);
      return new TestManagedSeriesReader(batchData, filter);
    }

    @Override
    public TSDataType getSeriesType(String pathStr) {
      return TSDataType.DOUBLE;
    }
  };
  private ClusterTimeGenerator timeGenerator;

  @Test
  public void test() throws StorageEngineException, IOException {
    QueryContext context = new QueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    IExpression expression =
        BinaryExpression.and(
            new SingleSeriesExpression(new Path(TestUtils.getTestSeries(0, 0)),
                ValueFilter.gtEq(3.0)),
            new SingleSeriesExpression(new Path(TestUtils.getTestSeries(1, 1)),
                ValueFilter.ltEq(8.0)));
    timeGenerator = new ClusterTimeGenerator(expression, context, metaGroupMember);
    for (int i = 3; i < 8; i++) {
      assertTrue(timeGenerator.hasNext());
      assertEquals(i, timeGenerator.next());
    }
  }

}