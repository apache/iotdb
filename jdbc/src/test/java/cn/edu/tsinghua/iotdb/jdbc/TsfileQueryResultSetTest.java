package cn.edu.tsinghua.iotdb.jdbc;

import cn.edu.tsinghua.service.rpc.thrift.*;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/*
    This class is designed to test the function of TsfileQueryResultSet.
    This class also sheds light on the complete execution process of a query sql from the jdbc perspective.

    The test utilizes the mockito framework to mock responses from an IoTDB server.
    The status of the IoTDB server mocked here is determined by the following sql commands:

    "SET STORAGE GROUP TO root.vehicle",
    "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
    "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
    "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
    "insert into root.vehicle.d0(timestamp,s0) values(1,101)",
    "insert into root.vehicle.d0(timestamp,s0) values(2,198)",
    "insert into root.vehicle.d0(timestamp,s0) values(100,99)",
    "insert into root.vehicle.d0(timestamp,s0) values(101,99)",
    "insert into root.vehicle.d0(timestamp,s0) values(102,80)",
    "insert into root.vehicle.d0(timestamp,s0) values(103,99)",
    "insert into root.vehicle.d0(timestamp,s0) values(104,90)",
    "insert into root.vehicle.d0(timestamp,s0) values(105,99)",
    "insert into root.vehicle.d0(timestamp,s0) values(106,99)",
    "insert into root.vehicle.d0(timestamp,s0) values(2,10000)",
    "insert into root.vehicle.d0(timestamp,s0) values(50,10000)",
    "insert into root.vehicle.d0(timestamp,s0) values(1000,22222)",
    "DELETE FROM root.vehicle.d0.s0 WHERE time < 104",
    "UPDATE root.vehicle.d0 SET s0 = 33333 WHERE time < 106 and time > 103",
    "insert into root.vehicle.d0(timestamp,s1) values(1,1101)",
    "insert into root.vehicle.d0(timestamp,s1) values(2,198)",
    "insert into root.vehicle.d0(timestamp,s1) values(100,199)",
    "insert into root.vehicle.d0(timestamp,s1) values(101,199)",
    "insert into root.vehicle.d0(timestamp,s1) values(102,180)",
    "insert into root.vehicle.d0(timestamp,s1) values(103,199)",
    "insert into root.vehicle.d0(timestamp,s1) values(104,190)",
    "insert into root.vehicle.d0(timestamp,s1) values(105,199)",
    "insert into root.vehicle.d0(timestamp,s1) values(2,40000)",
    "insert into root.vehicle.d0(timestamp,s1) values(50,50000)",
    "insert into root.vehicle.d0(timestamp,s1) values(1000,55555)",
    "insert into root.vehicle.d0(timestamp,s2) values(1000,55555)",
    "insert into root.vehicle.d0(timestamp,s2) values(2,2.22)",
    "insert into root.vehicle.d0(timestamp,s2) values(3,3.33)",
    "insert into root.vehicle.d0(timestamp,s2) values(4,4.44)",
    "insert into root.vehicle.d0(timestamp,s2) values(102,10.00)",
    "insert into root.vehicle.d0(timestamp,s2) values(105,11.11)",
    "insert into root.vehicle.d0(timestamp,s2) values(1000,1000.11)",
    "insert into root.vehicle.d0(timestamp,s1) values(2000-01-01T08:00:00+08:00, 100)",
 */

public class TsfileQueryResultSetTest {
    @Mock
    private TsfileConnection connection;
    @Mock
    private TSIService.Iface client;
    @Mock
    private TS_SessionHandle sessHandle;
    @Mock
    private Statement statement;
    @Mock
    TSExecuteStatementResp execResp;
    @Mock
    TSOperationHandle operationHandle;
    @Mock
    private TSFetchMetadataResp fetchMetadataResp;
    @Mock
    private TSFetchResultsResp fetchResultsResp;

    private TS_Status Status_SUCCESS = new TS_Status(TS_StatusCode.SUCCESS_STATUS);

    @Before
    public void before() throws Exception {
        MockitoAnnotations.initMocks(this);

        statement = new TsfileStatement(connection, client, sessHandle);

        when(connection.isClosed()).thenReturn(false);
        when(client.executeStatement(any(TSExecuteStatementReq.class))).thenReturn(execResp);
        operationHandle.hasResultSet = true;
        when(execResp.getOperationHandle()).thenReturn(operationHandle);
        when(execResp.getStatus()).thenReturn(Status_SUCCESS);

        when(client.fetchMetadata(any(TSFetchMetadataReq.class))).thenReturn(fetchMetadataResp);
        when(fetchMetadataResp.getStatus()).thenReturn(Status_SUCCESS);

        when(client.fetchResults(any(TSFetchResultsReq.class))).thenReturn(fetchResultsResp);
        when(fetchResultsResp.getStatus()).thenReturn(Status_SUCCESS);
    }

    @SuppressWarnings("resource")
    @Test
    public void testQuery() throws Exception {

        String testSql = "select *,s1,s0,s2 from root.vehicle.d0 where s1 > 190 or s2 < 10.0 " +
                "limit 20 offset 1 slimit 4 soffset 2";

        /*
            step 1: execute statement
         */
        List<String> columns = new ArrayList<>();
        columns.add("root.vehicle.d0.s2");
        columns.add("root.vehicle.d0.s1");
        columns.add("root.vehicle.d0.s0");
        columns.add("root.vehicle.d0.s2");

        when(execResp.getColumns()).thenReturn(columns);
        when(execResp.getOperationType()).thenReturn("QUERY");
        doReturn("FLOAT").doReturn("INT64").doReturn("INT32").doReturn("FLOAT")
                .when(fetchMetadataResp).getDataType();

        boolean hasResultSet = statement.execute(testSql);

        verify(fetchMetadataResp, times(4)).getDataType();

        /*
            step 2: fetch result
         */
        fetchResultsResp.hasResultSet = true; // at the first time to fetch
        TSQueryDataSet tsQueryDataSet = FakedFirstFetchResult();
        when(fetchResultsResp.getQueryDataSet()).thenReturn(tsQueryDataSet);

        if (hasResultSet) {
            ResultSet resultSet = statement.getResultSet();
            // check columnInfoMap
            Assert.assertEquals(resultSet.findColumn("Time"), 1);
            Assert.assertEquals(resultSet.findColumn("root.vehicle.d0.s2"), 2);
            Assert.assertEquals(resultSet.findColumn("root.vehicle.d0.s1"), 3);
            Assert.assertEquals(resultSet.findColumn("root.vehicle.d0.s0"), 4);

            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            // check columnInfoList
            Assert.assertEquals(resultSetMetaData.getColumnName(1), "Time");
            Assert.assertEquals(resultSetMetaData.getColumnName(2), "root.vehicle.d0.s2");
            Assert.assertEquals(resultSetMetaData.getColumnName(3), "root.vehicle.d0.s1");
            Assert.assertEquals(resultSetMetaData.getColumnName(4), "root.vehicle.d0.s0");
            Assert.assertEquals(resultSetMetaData.getColumnName(5), "root.vehicle.d0.s2");
            // check columnTypeList
            Assert.assertEquals(resultSetMetaData.getColumnType(1), Types.TIMESTAMP);
            Assert.assertEquals(resultSetMetaData.getColumnType(2), Types.FLOAT);
            Assert.assertEquals(resultSetMetaData.getColumnType(3), Types.BIGINT);
            Assert.assertEquals(resultSetMetaData.getColumnType(4), Types.INTEGER);
            Assert.assertEquals(resultSetMetaData.getColumnType(5), Types.FLOAT);
            // check fetched result
            int colCount = resultSetMetaData.getColumnCount();
            StringBuilder resultStr = new StringBuilder();
            for (int i = 1; i < colCount + 1; i++) { // meta title
                resultStr.append(resultSetMetaData.getColumnName(i)).append(",");
            }
            resultStr.append("\n");
            while (resultSet.next()) { // data
                for (int i = 1; i <= colCount; i++) {
                    resultStr.append(resultSet.getString(i)).append(",");
                }
                resultStr.append("\n");

                fetchResultsResp.hasResultSet = false; // at the second time to fetch
            }
            String standard = "Time,root.vehicle.d0.s2,root.vehicle.d0.s1,root.vehicle.d0.s0,root.vehicle.d0.s2,\n" +
                    "2,2.22,40000,null,2.22,\n" +
                    "3,3.33,null,null,3.33,\n" +
                    "4,4.44,null,null,4.44,\n" +
                    "50,null,50000,null,null,\n" +
                    "100,null,199,null,null,\n" +
                    "101,null,199,null,null,\n" +
                    "103,null,199,null,null,\n" +
                    "105,11.11,199,33333,11.11,\n" +
                    "1000,1000.11,55555,22222,1000.11,\n";
            Assert.assertEquals(resultStr.toString(), standard);
        }
    }

    // fake the first-time fetched result of 'testSql' from an IoTDB server
    private TSQueryDataSet FakedFirstFetchResult() {
        TSQueryDataSet tsQueryDataSet = new TSQueryDataSet(new ArrayList<>());
        final int DATA_TYPE_NUM = 3;
        Object[][] input = {
                {
                        1L,
                        "root.vehicle.d0.s2", TSDataType.FLOAT, null,
                        "root.vehicle.d0.s1", TSDataType.INT64, 1101L,
                        "root.vehicle.d0.s0", TSDataType.INT32, null,
                },
                {
                        2L,
                        "root.vehicle.d0.s2", TSDataType.FLOAT, 2.22F,
                        "root.vehicle.d0.s1", TSDataType.INT64, 40000L,
                        "root.vehicle.d0.s0", TSDataType.INT32, null,
                },
                {
                        3L,
                        "root.vehicle.d0.s2", TSDataType.FLOAT, 3.33F,
                        "root.vehicle.d0.s1", TSDataType.INT64, null,
                        "root.vehicle.d0.s0", TSDataType.INT32, null,
                },
                {
                        4L,
                        "root.vehicle.d0.s2", TSDataType.FLOAT, 4.44F,
                        "root.vehicle.d0.s1", TSDataType.INT64, null,
                        "root.vehicle.d0.s0", TSDataType.INT32, null,
                },
                {
                        50L,
                        "root.vehicle.d0.s2", TSDataType.FLOAT, null,
                        "root.vehicle.d0.s1", TSDataType.INT64, 50000L,
                        "root.vehicle.d0.s0", TSDataType.INT32, null,
                },
                {
                        100L,
                        "root.vehicle.d0.s2", TSDataType.FLOAT, null,
                        "root.vehicle.d0.s1", TSDataType.INT64, 199L,
                        "root.vehicle.d0.s0", TSDataType.INT32, null,
                },
                {
                        101L,
                        "root.vehicle.d0.s2", TSDataType.FLOAT, null,
                        "root.vehicle.d0.s1", TSDataType.INT64, 199L,
                        "root.vehicle.d0.s0", TSDataType.INT32, null,
                },
                {
                        103L,
                        "root.vehicle.d0.s2", TSDataType.FLOAT, null,
                        "root.vehicle.d0.s1", TSDataType.INT64, 199L,
                        "root.vehicle.d0.s0", TSDataType.INT32, null,
                },
                {
                        105L,
                        "root.vehicle.d0.s2", TSDataType.FLOAT, 11.11F,
                        "root.vehicle.d0.s1", TSDataType.INT64, 199L,
                        "root.vehicle.d0.s0", TSDataType.INT32, 33333,
                },
                {
                        1000L,
                        "root.vehicle.d0.s2", TSDataType.FLOAT, 1000.11F,
                        "root.vehicle.d0.s1", TSDataType.INT64, 55555L,
                        "root.vehicle.d0.s0", TSDataType.INT32, 22222,
                }
        };
        for (Object[] item : input) {
            TSRowRecord record = new TSRowRecord();
            record.setTimestamp((long) item[0]);
            List<String> keys = new ArrayList<>();
            List<TSDataValue> values = new ArrayList<>();
            for (int i = 0; i < DATA_TYPE_NUM; i++) {
                keys.add((String) item[3 * i + 1]);
                TSDataValue value = new TSDataValue(false);
                if (item[3 * i + 3] == null) {
                    value.setIs_empty(true);
                } else {
                    if (i == 0) {
                        value.setFloat_val((float) item[3 * i + 3]);
                        value.setType(item[3 * i + 2].toString());
                    } else if (i == 1) {
                        value.setLong_val((long) item[3 * i + 3]);
                        value.setType(item[3 * i + 2].toString());
                    } else {
                        value.setInt_val((int) item[3 * i + 3]);
                        value.setType(item[3 * i + 2].toString());
                    }
                }
                values.add(value);
            }
            record.setKeys(keys);
            record.setValues(values);
            tsQueryDataSet.getRecords().add(record);
        }
        return tsQueryDataSet;
    }
}