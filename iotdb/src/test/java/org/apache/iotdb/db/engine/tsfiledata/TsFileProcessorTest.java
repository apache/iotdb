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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.EngineExecutorWithoutTimeGenerator;
import org.apache.iotdb.db.query.executor.EngineQueryRouter;
import org.apache.iotdb.db.utils.EnvironmentUtils;
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
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TsFileProcessorTest {
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

  @Before
  public void setUp() throws Exception {
    mManager = MManager.getInstance();
    queryManager = new EngineQueryRouter();
    measurementSchemaMap.put("s1", new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    measurementSchemaMap.put("s2", new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    measurementSchemaMap.put("s3", new MeasurementSchema("s3", TSDataType.FLOAT, TSEncoding.RLE));
    schema = new FileSchema(measurementSchemaMap);
    processor = new TsFileProcessor("root.test", doNothingAction, doNothingAction, doNothingAction,
        SysTimeVersionController.INSTANCE, schema);
  }


  @After
  public void tearDown() throws Exception {
    //processor.close();
   processor.removeMe();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void insert()
      throws BufferWriteProcessorException, IOException, ExecutionException, InterruptedException, FileNodeProcessorException, FileNodeManagerException, PathErrorException, MetadataArgsErrorException {
    mManager.setStorageLevelToMTree("root.test");
    mManager.addPathToMTree("root.test.d1.s1",  TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY, Collections.emptyMap());
    System.out.println(processor.insert("root.test.d1", "s1", 10, TSDataType.FLOAT, "5.0"));
    System.out.println(processor.insert("root.test.d1", "s2", 10, TSDataType.FLOAT, "5.0"));
    System.out.println(processor.insert("root.test.d1", "s1", 12, TSDataType.FLOAT, "5.0"));
    Future<Boolean> ok = processor.flush();
    ok.get();
    ok = processor.flush();
    ok.get();
    ok = processor.flush();
    ok.get();
    processor.delete("root.test.d1", "s1",12);
    //let's rewrite timestamp =12 again..
    System.out.println(processor.insert("root.test.d1", "s1", 12, TSDataType.FLOAT, "5.0"));
    System.out.println(processor.insert("root.test.d1", "s1", 12, TSDataType.FLOAT, "5.0"));
    System.out.println(processor.insert("root.test.d1", "s1", 13, TSDataType.FLOAT, "5.0"));
    System.out.println(processor.insert("root.test.d2", "s1", 10, TSDataType.FLOAT, "5.0"));
    System.out.println(processor.insert(new TSRecord(14, "root.test.d1").addTuple(new FloatDataPoint("s1", 6.0f))));
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
  public void delete() {

  }


  @Test
  public void canBeClosed() {
  }


  @Test
  public void memoryUsage() {
  }

  @Test
  public void isFlush() {
    //processor.isFlush() can not be tested... because it is uncertain...
  }

  @Test
  public void queryBufferWriteData() {
  }


}
