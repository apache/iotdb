/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.iotdb.service.rpc.thrift.TSExecuteBatchStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteBatchStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TS_SessionHandle;
import org.apache.iotdb.service.rpc.thrift.TS_Status;
import org.apache.iotdb.service.rpc.thrift.TS_StatusCode;

public class BatchTest {
    @Mock
    private IoTDBConnection connection;
    @Mock
    private TSIService.Iface client;
    @Mock
    private TS_SessionHandle sessHandle;
    private TS_Status Status_SUCCESS = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
    private TS_Status Status_ERROR = new TS_Status(TS_StatusCode.ERROR_STATUS);
    private TSExecuteBatchStatementResp resp;
    private ZoneId zoneID = ZoneId.systemDefault();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(connection.createStatement()).thenReturn(new IoTDBStatement(connection, client, sessHandle, zoneID));
    }

    @After
    public void tearDown() throws Exception {
    }

    @SuppressWarnings("serial")
    @Test
    public void testExecuteBatchSQL1() throws SQLException, TException {
        Statement statement = connection.createStatement();
        resp = new TSExecuteBatchStatementResp(Status_SUCCESS);
        when(client.executeBatchStatement(any(TSExecuteBatchStatementReq.class))).thenReturn(resp);
        int[] result = statement.executeBatch();
        assertEquals(result.length, 0);

        List<Integer> resExpected = new ArrayList<Integer>() {
            {
                add(Statement.SUCCESS_NO_INFO);
                add(Statement.SUCCESS_NO_INFO);
                add(Statement.SUCCESS_NO_INFO);
                add(Statement.SUCCESS_NO_INFO);
                add(Statement.SUCCESS_NO_INFO);
                add(Statement.EXECUTE_FAILED);
                add(Statement.SUCCESS_NO_INFO);
                add(Statement.SUCCESS_NO_INFO);
                add(Statement.EXECUTE_FAILED);
            }
        };
        resp.setResult(resExpected);

        statement.addBatch("SET STORAGE GROUP TO root.ln.wf01.wt01");
        statement.addBatch("CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
        statement.addBatch("CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE");
        statement.addBatch("insert into root.ln.wf01.wt01(timestamp,status) values(1509465600000,true)");
        statement.addBatch("insert into root.ln.wf01.wt01(timestamp,status) values(1509465660000,true)");
        statement.addBatch("insert into root.ln.wf01.wt01(timestamp,status) vvvvvv(1509465720000,false)");
        statement.addBatch("insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465600000,25.957603)");
        statement.addBatch("insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465660000,24.359503)");
        statement.addBatch("insert into root.ln.wf01.wt01(timestamp,temperature) vvvvvv(1509465720000,20.092794)");
        result = statement.executeBatch();
        assertEquals(result.length, resExpected.size());
        for (int i = 0; i < resExpected.size(); i++) {
            assertEquals(result[i], (int) resExpected.get(i));
        }
        statement.clearBatch();
    }

    @Test(expected = BatchUpdateException.class)
    public void testExecuteBatchSQL2() throws SQLException, TException {
        Statement statement = connection.createStatement();
        resp = new TSExecuteBatchStatementResp(Status_ERROR);
        when(client.executeBatchStatement(any(TSExecuteBatchStatementReq.class))).thenReturn(resp);
        statement.executeBatch();
    }

    @SuppressWarnings("serial")
    @Test
    public void testExecuteBatchSQL3() throws SQLException, TException {
        Statement statement = connection.createStatement();
        resp = new TSExecuteBatchStatementResp(Status_ERROR);
        List<Integer> resExpected = new ArrayList<Integer>() {
            {
                add(Statement.SUCCESS_NO_INFO);
                add(Statement.EXECUTE_FAILED);
            }
        };
        resp.setResult(resExpected);
        when(client.executeBatchStatement(any(TSExecuteBatchStatementReq.class))).thenReturn(resp);
        try {
            statement.executeBatch();
        } catch (BatchUpdateException e) {
            int[] result = e.getUpdateCounts();
            for (int i = 0; i < resExpected.size(); i++) {
                assertEquals(result[i], (int) resExpected.get(i));
            }
            return;
        }
        fail();
    }
}
