/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.flink;

import org.apache.iotdb.flink.options.IoTDBSinkOptions;
import org.apache.iotdb.session.pool.SessionPool;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class IoTDBSinkInsertTest {

  private IoTDBSink ioTDBSink;
  private SessionPool pool;

  @Before
  public void setUp() throws Exception {
    IoTDBSinkOptions options = new IoTDBSinkOptions();
    options.setTimeseriesOptionList(
        Lists.newArrayList(new IoTDBSinkOptions.TimeseriesOption("root.sg.D01.temperature")));
    ioTDBSink = new IoTDBSink(options, new DefaultIoTSerializationSchema());

    pool = mock(SessionPool.class);
    ioTDBSink.setSessionPool(pool);
  }

  @Test
  public void testInsert() throws Exception {
    Map<String, String> tuple = new HashMap();
    tuple.put("device", "root.sg.D01");
    tuple.put("timestamp", "1581861293000");
    tuple.put("measurements", "temperature");
    tuple.put("types", "DOUBLE");
    tuple.put("values", "36.5");

    ioTDBSink.invoke(tuple, null);
    verify(pool)
        .insertRecord(
            any(String.class), any(Long.class), any(List.class), any(List.class), any(List.class));
  }

  @Test
  public void close() throws Exception {
    ioTDBSink.close();
    verify(pool).close();
  }
}
