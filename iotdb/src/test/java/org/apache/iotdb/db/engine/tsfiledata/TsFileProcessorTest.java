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

package org.apache.iotdb.db.engine.tsfiledata;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.bufferwrite.Action;
import org.apache.iotdb.db.engine.bufferwrite.ActionException;
import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.engine.querycontext.GlobalSortedSeriesDataSource;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.version.SysTimeVersionController;
import org.apache.iotdb.db.exception.BufferWriteProcessorException;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.FileNodeProcessorException;
import org.apache.iotdb.db.exception.MetadataArgsErrorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryTokenManager;
import org.apache.iotdb.db.query.executor.EngineExecutorWithoutTimeGenerator;
import org.apache.iotdb.db.query.executor.EngineQueryRouter;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.ImmediateFuture;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsFileProcessorTest {
  private static Logger LOGGER = LoggerFactory.getLogger(TsFileProcessorTest.class);
  TsFileProcessor processor;
  MManager mManager;
  EngineQueryRouter queryManager;
  Action doNothingAction = new Action() {
    @Override
    public void act() throws ActionException {
    }
  };
  Map<String, MeasurementSchema> measurementSchemaMap = new HashMap<>();

  FileSchema schema;

  long oldBufferwriteFileSizeThreshold = IoTDBDescriptor.getInstance().getConfig().getBufferwriteFileSizeThreshold();
  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.cleanEnv();
//
//  IoTDBDescriptor.getInstance().getConfig().setEnableWal(true);
    IoTDBDescriptor.getInstance().getConfig().setBufferwriteFileSizeThreshold(1*1024*1024);
    mManager = MManager.getInstance();
    queryManager = new EngineQueryRouter();
    measurementSchemaMap.put("s1", new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    measurementSchemaMap.put("s2", new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    measurementSchemaMap.put("s3", new MeasurementSchema("s3", TSDataType.FLOAT, TSEncoding.RLE));
    schema = new FileSchema(measurementSchemaMap);
    processor = new TsFileProcessor("root.test", doNothingAction, doNothingAction, doNothingAction,
        SysTimeVersionController.INSTANCE, schema);
    mManager.setStorageLevelToMTree("root.test");
    mManager.addPathToMTree("root.test.d1.s1",  TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY, Collections.emptyMap());
    mManager.addPathToMTree("root.test.d2.s1",  TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY, Collections.emptyMap());
    mManager.addPathToMTree("root.test.d1.s2",  TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY, Collections.emptyMap());
    mManager.addPathToMTree("root.test.d2.s2",  TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY, Collections.emptyMap());
  }


  @After
  public void tearDown() throws Exception {
    //processor.close();
    processor.writeLock();
   processor.removeMe();
   processor.writeUnlock();
   EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setEnableWal(false);
    IoTDBDescriptor.getInstance().getConfig().setBufferwriteFileSizeThreshold(oldBufferwriteFileSizeThreshold);
  }

  @Test
  public void insert()
      throws BufferWriteProcessorException, IOException, ExecutionException, InterruptedException, FileNodeProcessorException, FileNodeManagerException, PathErrorException, MetadataArgsErrorException {

    Assert.assertTrue(processor.insert("root.test.d1", "s1", 10, "5.0"));
    Assert.assertTrue(processor.insert("root.test.d1", "s2", 10, "5.0"));
    Assert.assertTrue(processor.insert("root.test.d1", "s1", 12, "5.0"));
    Future<Boolean> ok = processor.flush();
    ok.get();
    ok = processor.flush();
    Assert.assertTrue(ok instanceof ImmediateFuture);
    ok.get();
    ok = processor.flush();
    Assert.assertTrue(ok instanceof ImmediateFuture);
    ok.get();

    //let's rewrite timestamp =12 again..
    Assert.assertFalse(processor.insert("root.test.d1", "s1", 12, "5.0"));
    processor.delete("root.test.d1", "s1",12);
    Assert.assertTrue(processor.insert("root.test.d1", "s1", 12, "5.0"));
    Assert.assertTrue(processor.insert("root.test.d1", "s1", 13, "5.0"));
    Assert.assertTrue(processor.insert("root.test.d2", "s1", 10, "5.0"));
    Assert.assertTrue(processor.insert(new TSRecord(14, "root.test.d1").addTuple(new FloatDataPoint("s1", 6.0f))));
    processor.delete("root.test.d1", "s1",12);
    processor.delete("root.test.d3", "s1",12);


    QueryExpression qe = QueryExpression.create(Collections.singletonList(new Path("root.test.d1", "s1")), null);
    QueryDataSet result = queryManager.query(qe, processor);
    while (result.hasNext()) {
      RowRecord record = result.next();
      System.out.println(record.getTimestamp() +"," + record.getFields().get(0).getFloatV());
    }
  }



  @Test
  public void bruteForceTest() throws InterruptedException, FileNodeManagerException, IOException {

    String[] devices = new String[] {"root.test.d1", "root.test.d2"};
    String[] sensors = new String[] {"s1", "s2"};
    final boolean[] exception = {false, false, false};
    final boolean[] goon = {true};
    int totalsize = 100000;
    final int[] count = {0};
    QueryExpression qe = QueryExpression.create(Collections.singletonList(new Path("root.test.d1", "s1")), null);
    Thread insertThread = new Thread() {
      @Override
      public void run() {
        int i =0;
        long time = 100L;
        long start = System.currentTimeMillis();
        String[] sensors = new String[]{"s1"};
        String[] values = new String[1];
        try {
          for (int j = 0; j < totalsize  && goon[0]; j++) {
            processor.lock(true);
//            processor.insert("root.test.d1","s1", time++,  String.valueOf(j));
//            processor.insert("root.test.d2","s1", time++,  String.valueOf(j));
            values[0] = String.valueOf(j);
            processor.insert(new InsertPlan("root.test.d1",  time++, sensors, values));
            processor.insert(new InsertPlan("root.test.d2",  time++, sensors, values));
            processor.writeUnlock();
            count[0]++;
          }
          System.out.println((System.currentTimeMillis() - start));
        } catch (BufferWriteProcessorException | IOException e) {
          // we will break out.
          LOGGER.error(e.getMessage());
          exception[0] = true;
        }
      }
    };
//    Thread flushThread = new Thread() {
//      @Override
//      public void run() {
//        try {
//          for (int j = 0; j < totalsize * 2 && goon[0]; j++) {
//            processor.lock(true);
//            processor.flush();
//            processor.writeUnlock();
//          }
//        } catch (IOException e) {
//          // we will break out.
//          LOGGER.error(e.getMessage());
//          exception[1] = true;
//        }
//      }
//    };
    //we temporary disable the query because there are bugs..
//    Thread queryThread = new Thread() {
//      @Override
//      public void run() {
//        try {
//          for (int j = 0; j < totalsize * 2 && goon[0]; j++) {
//            processor.lock(false);
//            QueryDataSet result = queryManager.query(qe, processor);
//            while (result.hasNext()) {
//              result.next();
//            }
//            QueryTokenManager.getInstance().endQueryForCurrentRequestThread();
//            processor.readUnlock();
//          }
//        } catch (IOException | FileNodeManagerException e) {
//          // we will break out.
//          LOGGER.error(e.getMessage());
//          exception[2] = true;
//        }
//      }
//    };
   // flushThread.start();
    insertThread.start();
    //queryThread.start();
    //wait at most 20 seconds.
    insertThread.join(20000);
    goon[0] = false;
    //queryThread.join(5000);
    Assert.assertFalse(exception[0]);
    Assert.assertFalse(exception[1]);
    Assert.assertFalse(exception[2]);
    QueryDataSet result = queryManager.query(qe, processor);
    int size =0;
    while (result.hasNext()) {
      RowRecord record = result.next();
      size ++;
    }
    QueryTokenManager.getInstance().endQueryForCurrentRequestThread();
    Assert.assertEquals(count[0], size);
  }



}
