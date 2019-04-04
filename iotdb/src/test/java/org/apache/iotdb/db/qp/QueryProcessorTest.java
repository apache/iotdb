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
package org.apache.iotdb.db.qp;

import static org.junit.Assert.assertEquals;

import org.apache.iotdb.db.exception.ArgsErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.qp.QueryProcessorException;
import org.apache.iotdb.db.qp.executor.OverflowQPExecutor;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.junit.Test;

public class QueryProcessorTest {

  private QueryProcessor processor = new QueryProcessor(new OverflowQPExecutor());

  @Test
  public void parseSQLToPhysicalPlan()
      throws ArgsErrorException, ProcessorException, QueryProcessorException {
    String createSGStatement = "set storage group to root.vehicle";
    PhysicalPlan plan1 = processor.parseSQLToPhysicalPlan(createSGStatement);
    assertEquals(OperatorType.SET_STORAGE_GROUP, plan1.getOperatorType());

    String createTSStatement = "create timeseries root.vehicle.d1.s1 with datatype=INT32,encoding=RLE";
    PhysicalPlan plan2 = processor.parseSQLToPhysicalPlan(createTSStatement);
    assertEquals(OperatorType.CREATE_TIMESERIES, plan2.getOperatorType());

    String deleteTSStatement = "delete timeseries root.vehicle.d1.s1";
    PhysicalPlan plan3 = processor.parseSQLToPhysicalPlan(deleteTSStatement);
    assertEquals(OperatorType.DELETE_TIMESERIES, plan3.getOperatorType());

    String insertStatement = "insert into root.vehicle.d0(timestamp,s0) values(10,100)";
    PhysicalPlan plan4 = processor.parseSQLToPhysicalPlan(insertStatement);
    assertEquals(OperatorType.INSERT, plan4.getOperatorType());

    String propertyStatement = "add label label1021 to property propropro";
    PhysicalPlan plan5 = processor.parseSQLToPhysicalPlan(propertyStatement);
    assertEquals(OperatorType.PROPERTY, plan5.getOperatorType());

    String deleteStatement = "DELETE FROM root.device0.sensor0,root.device0.sensor1 WHERE time <= 5000";
    PhysicalPlan plan6 = processor.parseSQLToPhysicalPlan(deleteStatement);
    assertEquals(OperatorType.DELETE, plan6.getOperatorType());

  }
}