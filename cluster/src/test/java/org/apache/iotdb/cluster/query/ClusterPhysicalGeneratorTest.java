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

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.crud.FromOperator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.crud.SelectOperator;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.Before;
import org.junit.Test;

public class ClusterPhysicalGeneratorTest extends BaseQueryTest{

  private ClusterPhysicalGenerator physicalGenerator;

  @Before
  public void setUp() throws MetadataException {
    super.setUp();
    physicalGenerator = new ClusterPhysicalGenerator(queryProcessExecutor, metaGroupMember);
  }

  @Test
  public void test() throws QueryProcessException {
    QueryOperator operator = new QueryOperator(SQLConstant.TOK_QUERY);
    operator.setGroupByDevice(true);

    SelectOperator selectOperator = new SelectOperator(SQLConstant.TOK_SELECT);
    selectOperator.setSuffixPathList(Collections.singletonList(new Path("*")));
    FromOperator fromOperator = new FromOperator(SQLConstant.TOK_FROM);
    fromOperator.addPrefixTablePath(new Path(TestUtils.getTestSg(0)));

    operator.setSelectOperator(selectOperator);
    operator.setFromOperator(fromOperator);
    QueryPlan plan = (QueryPlan) physicalGenerator.transformToPhysicalPlan(operator);

    // TODO: Enable this when IOTDB-412 is fixed
    //assertEquals(pathList, plan.getDeduplicatedPaths());
    assertEquals(dataTypes, plan.getDeduplicatedDataTypes());
  }
}