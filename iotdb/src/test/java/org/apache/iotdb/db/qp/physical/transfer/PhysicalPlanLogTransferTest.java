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
package org.apache.iotdb.db.qp.physical.transfer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.exception.ArgsErrorException;
import org.apache.iotdb.db.exception.MetadataErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.qp.QueryProcessorException;
import org.apache.iotdb.db.qp.QueryProcessor;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.UpdatePlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadDataPlan;
import org.apache.iotdb.db.qp.physical.sys.MetadataPlan;
import org.apache.iotdb.db.qp.physical.sys.PropertyPlan;
import org.apache.iotdb.db.qp.utils.MemIntQpExecutor;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.Test;

public class PhysicalPlanLogTransferTest {

  private QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
  private InsertPlan insertPlan = new InsertPlan(1, "device", 100,
      new String[]{"s1", "s2", "s3", "s4"}, new String[]{"0.1", "100", "test", "false"});
  private DeletePlan deletePlan = new DeletePlan(50, new Path("root.vehicle.device"));
  private UpdatePlan updatePlan = new UpdatePlan(0, 100, "2.0",
      new Path("root.vehicle.device.sensor"));
  private LoadDataPlan loadDataPlan = new LoadDataPlan("/tmp/data/vehicle", "sensor");

  @Test
  public void operatorToLog()
      throws IOException, ArgsErrorException, ProcessorException, QueryProcessorException,
      MetadataErrorException {
    /** Insert Plan test **/
    byte[] insertPlanBytesTest = PhysicalPlanLogTransfer.planToLog(insertPlan);
    Codec<InsertPlan> insertPlanCodec = CodecInstances.multiInsertPlanCodec;
    byte[] insertPlanProperty = insertPlanCodec.encode(insertPlan);
    assertArrayEquals(insertPlanProperty, insertPlanBytesTest);

    /** Delete Plan test **/
    byte[] deletePlanBytesTest = PhysicalPlanLogTransfer.planToLog(deletePlan);
    Codec<DeletePlan> deletePlanCodec = CodecInstances.deletePlanCodec;
    byte[] deletePlanProperty = deletePlanCodec.encode(deletePlan);
    assertArrayEquals(deletePlanProperty, deletePlanBytesTest);

    /** Update Plan test **/
    byte[] updatePlanBytesTest = PhysicalPlanLogTransfer.planToLog(updatePlan);
    Codec<UpdatePlan> updatePlanCodec = CodecInstances.updatePlanCodec;
    byte[] updatePlanProperty = updatePlanCodec.encode(updatePlan);
    assertArrayEquals(updatePlanProperty, updatePlanBytesTest);

    /** Metadata Plan test **/
    String metadataStatement = "create timeseries root.vehicle.d1.s1 with datatype=INT32,encoding=RLE";
    MetadataPlan metadataPlan = (MetadataPlan) processor.parseSQLToPhysicalPlan(metadataStatement);
    byte[] metadataPlanBytesTest = PhysicalPlanLogTransfer.planToLog(metadataPlan);
    Codec<MetadataPlan> metadataPlanCodec = CodecInstances.metadataPlanCodec;
    byte[] metadataPlanProperty = metadataPlanCodec.encode(metadataPlan);
    assertArrayEquals(metadataPlanProperty, metadataPlanBytesTest);

    /** Author Plan test **/
    String sql = "grant role xm privileges 'SET_STORAGE_GROUP','DELETE_TIMESERIES' on root.vehicle.device.sensor";
    AuthorPlan authorPlan = (AuthorPlan) processor.parseSQLToPhysicalPlan(sql);
    byte[] authorPlanBytesTest = PhysicalPlanLogTransfer.planToLog(authorPlan);
    Codec<AuthorPlan> authorPlanCodec = CodecInstances.authorPlanCodec;
    byte[] authorPlanProperty = authorPlanCodec.encode(authorPlan);
    assertArrayEquals(authorPlanProperty, authorPlanBytesTest);

    /** LoadData Plan test **/
    byte[] loadDataPlanBytesTest = PhysicalPlanLogTransfer.planToLog(loadDataPlan);
    Codec<LoadDataPlan> loadDataPlanCodec = CodecInstances.loadDataPlanCodec;
    byte[] loadDataPlanProperty = loadDataPlanCodec.encode(loadDataPlan);
    assertArrayEquals(loadDataPlanProperty, loadDataPlanBytesTest);

    /** Property Plan test **/
    sql = "add label label1021 to property propropro";
    PropertyPlan propertyPlan = (PropertyPlan) processor.parseSQLToPhysicalPlan(sql);
    byte[] propertyPlanBytesTest = PhysicalPlanLogTransfer.planToLog(propertyPlan);
    Codec<PropertyPlan> propertyPlanCodec = CodecInstances.propertyPlanCodec;
    byte[] propertyPlanProperty = propertyPlanCodec.encode(propertyPlan);
    assertArrayEquals(propertyPlanProperty, propertyPlanBytesTest);

  }

  @Test
  public void logToOperator()
      throws IOException, ArgsErrorException, ProcessorException, QueryProcessorException,
      AuthException, MetadataErrorException {

    /** Insert Plan test **/
    byte[] insertPlanBytesTest = PhysicalPlanLogTransfer.planToLog(insertPlan);
    InsertPlan insertPlanTest = (InsertPlan) PhysicalPlanLogTransfer
        .logToPlan(insertPlanBytesTest);
    assertEquals(insertPlanTest, insertPlan);

    /** Delete Plan test **/
    byte[] deletePlanBytesTest = PhysicalPlanLogTransfer.planToLog(deletePlan);
    DeletePlan deletePlanTest = (DeletePlan) PhysicalPlanLogTransfer
        .logToPlan(deletePlanBytesTest);
    assertEquals(deletePlanTest, deletePlan);

    /** Update Plan test **/
    byte[] updatePlanBytesTest = PhysicalPlanLogTransfer.planToLog(updatePlan);
    UpdatePlan updatePlanTest = (UpdatePlan) PhysicalPlanLogTransfer
        .logToPlan(updatePlanBytesTest);
    assertEquals(updatePlanTest, updatePlan);

    /** Metadata Plan test **/
    String metadataStatement = "create timeseries root.vehicle.d1.s1 with datatype=INT32,encoding=RLE";
    MetadataPlan metadataPlan = (MetadataPlan) processor.parseSQLToPhysicalPlan(metadataStatement);
    byte[] metadataPlanBytesTest = PhysicalPlanLogTransfer.planToLog(metadataPlan);
    MetadataPlan metadataPlanTest = (MetadataPlan) PhysicalPlanLogTransfer
        .logToPlan(metadataPlanBytesTest);
    assertEquals(metadataPlanTest, metadataPlan);

    /** Author Plan test **/
    String sql = "grant role xm privileges 'SET_STORAGE_GROUP','DELETE_TIMESERIES' "
        + "on root.vehicle.device.sensor";
    AuthorPlan authorPlan = (AuthorPlan) processor.parseSQLToPhysicalPlan(sql);
    byte[] authorPlanBytesTest = PhysicalPlanLogTransfer.planToLog(authorPlan);
    AuthorPlan authorPlanTest = (AuthorPlan) PhysicalPlanLogTransfer
        .logToPlan(authorPlanBytesTest);
    assertEquals(authorPlanTest, authorPlan);

    /** LoadData Plan test **/
    byte[] loadDataPlanBytesTest = PhysicalPlanLogTransfer.planToLog(loadDataPlan);
    LoadDataPlan loadDataPlanTest = (LoadDataPlan) PhysicalPlanLogTransfer
        .logToPlan(loadDataPlanBytesTest);
    assertEquals(loadDataPlan, loadDataPlanTest);

    /** Property Plan test **/
    sql = "add label label1021 to property propropro";
    PropertyPlan propertyPlan = (PropertyPlan) processor.parseSQLToPhysicalPlan(sql);
    byte[] propertyPlanBytesTest = PhysicalPlanLogTransfer.planToLog(propertyPlan);
    PropertyPlan propertyPlanTest = (PropertyPlan) PhysicalPlanLogTransfer
        .logToPlan(propertyPlanBytesTest);
    assertEquals(propertyPlanTest, propertyPlan);

  }
}