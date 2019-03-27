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
package org.apache.iotdb.db.writelog.transfer;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.exception.ArgsErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.qp.QueryProcessorException;
import org.apache.iotdb.db.qp.QueryProcessor;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.UpdatePlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.MetadataPlan;
import org.apache.iotdb.db.qp.utils.MemIntQpExecutor;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.Test;

public class PhysicalPlanLogTransferTest {

  private QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
  private InsertPlan insertPlan = new InsertPlan(1, "device", 100,
      Arrays.asList("s1", "s2", "s3", "s4"), Arrays.asList("0.1", "100", "test", "false"));
  private DeletePlan deletePlan = new DeletePlan(50, new Path("root.vehicle.device"));
  private UpdatePlan updatePlan = new UpdatePlan(0, 100, "2.0",
      new Path("root.vehicle.device.sensor"));

  @Test
  public void operatorToLog()
      throws IOException, ArgsErrorException, ProcessorException, QueryProcessorException {
    byte[] insertPlanBytesTest = PhysicalPlanLogTransfer.operatorToLog(insertPlan);
    Codec<InsertPlan> insertPlanCodec = CodecInstances.multiInsertPlanCodec;
    byte[] insertPlanProperty = insertPlanCodec.encode(insertPlan);
    assertEquals(true, Arrays.equals(insertPlanProperty, insertPlanBytesTest));

    byte[] deletePlanBytesTest = PhysicalPlanLogTransfer.operatorToLog(deletePlan);
    Codec<DeletePlan> deletePlanCodec = CodecInstances.deletePlanCodec;
    byte[] deletePlanProperty = deletePlanCodec.encode(deletePlan);
    assertEquals(true, Arrays.equals(deletePlanProperty, deletePlanBytesTest));

    byte[] updatePlanBytesTest = PhysicalPlanLogTransfer.operatorToLog(updatePlan);
    Codec<UpdatePlan> updatePlanCodec = CodecInstances.updatePlanCodec;
    byte[] updatePlanProperty = updatePlanCodec.encode(updatePlan);
    assertEquals(true, Arrays.equals(updatePlanProperty, updatePlanBytesTest));

    String metadataStatement = "create timeseries root.vehicle.d1.s1 with datatype=INT32,encoding=RLE";
    MetadataPlan metadataPlan = (MetadataPlan) processor.parseSQLToPhysicalPlan(metadataStatement);
    byte[] metadataPlanBytesTest = PhysicalPlanLogTransfer.operatorToLog(metadataPlan);
    Codec<MetadataPlan> metadataPlanCodec = CodecInstances.metadataPlanCodec;
    byte[] metadataPlanProperty = metadataPlanCodec.encode(metadataPlan);
    assertEquals(true, Arrays.equals(metadataPlanProperty, metadataPlanBytesTest));

    String sql = "grant role xm privileges 'SET_STORAGE_GROUP','DELETE_TIMESERIES' on root.vehicle.device.sensor";
    AuthorPlan authorPlan = (AuthorPlan) processor.parseSQLToPhysicalPlan(sql);
    byte[] authorPlanBytesTest = PhysicalPlanLogTransfer.operatorToLog(authorPlan);
    Codec<AuthorPlan> authorPlanCodec = CodecInstances.authorPlanCodec;
    byte[] authorPlanProperty = authorPlanCodec.encode(authorPlan);
    assertEquals(true, Arrays.equals(authorPlanProperty, authorPlanBytesTest));

  }

  @Test
  public void logToOperator()
      throws IOException, ArgsErrorException, ProcessorException, QueryProcessorException, AuthException {

    byte[] insertPlanBytesTest = PhysicalPlanLogTransfer.operatorToLog(insertPlan);
    InsertPlan insertPlanTest = (InsertPlan) PhysicalPlanLogTransfer
        .logToOperator(insertPlanBytesTest);
    assertEquals(true, insertPlanTest.equals(insertPlan));

    byte[] deletePlanBytesTest = PhysicalPlanLogTransfer.operatorToLog(deletePlan);
    DeletePlan deletePlanTest = (DeletePlan) PhysicalPlanLogTransfer
        .logToOperator(deletePlanBytesTest);
    assertEquals(true, deletePlanTest.equals(deletePlan));

    byte[] updatePlanBytesTest = PhysicalPlanLogTransfer.operatorToLog(updatePlan);
    UpdatePlan updatePlanTest = (UpdatePlan) PhysicalPlanLogTransfer
        .logToOperator(updatePlanBytesTest);
    assertEquals(true, updatePlanTest.equals(updatePlan));

    String metadataStatement = "create timeseries root.vehicle.d1.s1 with datatype=INT32,encoding=RLE";
    MetadataPlan metadataPlan = (MetadataPlan) processor.parseSQLToPhysicalPlan(metadataStatement);
    byte[] metadataPlanBytesTest = PhysicalPlanLogTransfer.operatorToLog(metadataPlan);
    MetadataPlan metadataPlanTest = (MetadataPlan) PhysicalPlanLogTransfer
        .logToOperator(metadataPlanBytesTest);
    assertEquals(true, metadataPlanTest.equals(metadataPlan));

    String sql = "grant role xm privileges 'SET_STORAGE_GROUP','DELETE_TIMESERIES' on root.vehicle.device.sensor";
    AuthorPlan authorPlan = (AuthorPlan) processor.parseSQLToPhysicalPlan(sql);
    byte[] authorPlanBytesTest = PhysicalPlanLogTransfer.operatorToLog(authorPlan);
    AuthorPlan authorPlanTest = (AuthorPlan) PhysicalPlanLogTransfer.logToOperator(authorPlanBytesTest);
    assertEquals(true, authorPlanTest.equals(authorPlan));

  }
}