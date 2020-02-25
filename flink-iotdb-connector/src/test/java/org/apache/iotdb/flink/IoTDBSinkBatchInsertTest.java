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

import com.google.common.collect.Lists;
import org.apache.iotdb.session.Session;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class IoTDBSinkBatchInsertTest {

    private IoTDBSink ioTDBSink;
    private Session session;

    @Before
    public void setUp() throws Exception {
        IoTDBOptions options = new IoTDBOptions();
        options.setTimeseriesOptionList(Lists.newArrayList(new IoTDBOptions.TimeseriesOption("root.sg.D01.temperature")));
        ioTDBSink = new IoTDBSink(options, new DefaultIoTSerializationSchema(options));
        ioTDBSink.withBatchSize(3);

        session = mock(Session.class);
        ioTDBSink.setSession(session);
        ioTDBSink.setBatchList(new ArrayList<>());
    }

    @Test
    public void testBatchInsert() throws Exception {
        Map tuple = new HashMap();
        tuple.put("device", "root.sg.D01");
        tuple.put("timestamp", "1581861293000");
        tuple.put("measurements", "temperature");
        tuple.put("values", "36.5");
        ioTDBSink.invoke(tuple, null);

        verifyZeroInteractions(session);

        tuple = new HashMap();
        tuple.put("device", "root.sg.D01");
        tuple.put("timestamp", "1581861293001");
        tuple.put("measurements", "temperature");
        tuple.put("values", "37.2");
        ioTDBSink.invoke(tuple, null);

        verifyZeroInteractions(session);

        tuple = new HashMap();
        tuple.put("device", "root.sg.D01");
        tuple.put("timestamp", "1581861293003");
        tuple.put("measurements", "temperature");
        tuple.put("values", "37.1");
        ioTDBSink.invoke(tuple, null);

        verify(session).insertInBatch(any(List.class), any(List.class), any(List.class), any(List.class));

        tuple = new HashMap();
        tuple.put("device", "root.sg.D01");
        tuple.put("timestamp", "1581861293005");
        tuple.put("measurements", "temperature");
        tuple.put("values", "36.5");
        ioTDBSink.invoke(tuple, null);

        verifyZeroInteractions(session);
    }

    @Test
    public void close() throws Exception {
        Map tuple = new HashMap();
        tuple.put("device", "root.sg.D01");
        tuple.put("timestamp", "1581861293005");
        tuple.put("measurements", "temperature");
        tuple.put("values", "36.5");
        ioTDBSink.invoke(tuple, null);
        verifyZeroInteractions(session);

        ioTDBSink.close();
        verify(session).insertInBatch(any(List.class), any(List.class), any(List.class), any(List.class));
        verify(session).close();
    }

}
