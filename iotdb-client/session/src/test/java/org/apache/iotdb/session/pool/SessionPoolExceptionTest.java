package org.apache.iotdb.session.pool;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.pool.ISessionPool;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.reflect.Whitebox;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SessionPoolExceptionTest {

    @Mock private ISessionPool sessionPool;

    @Mock private Session session;

    @Before
    public void setUp(){
        MockitoAnnotations.initMocks(this);

        sessionPool = new SessionPool(Arrays.asList("host:11"), "user", "password", 10);
        ConcurrentLinkedDeque<ISession> queue = new ConcurrentLinkedDeque<>();
//        queue.add(session);

        // 设置 SessionPool 对象的内部状态
        Whitebox.setInternalState(sessionPool, "queue", queue);

//        Mockito.when()
    }

    @After
    public void tearDown() {
        // Close the session pool after each test
        if (null != sessionPool) {
            sessionPool.close();
        }
    }

    @Test(expected = IoTDBConnectionException.class)
    public void testInsertRecords() throws Exception {
        // 调用 insertRecords 方法
        List<String> deviceIds = Arrays.asList("device1", "device2");
        List<Long> timeList = Arrays.asList(1L, 2L);
        List<List<String>> measurementsList =
                Arrays.asList(
                        Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
        List<List<TSDataType>> typesList =
                Arrays.asList(
                        Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT),
                        Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE));
        List<List<Object>> valuesList =
                Arrays.asList(Arrays.asList(25.0f, 50.0f), Arrays.asList(220.0, 1.5));
        sessionPool.insertRecords(deviceIds, timeList, measurementsList, typesList, valuesList);
        assertEquals(
                1,
                ((ConcurrentLinkedDeque<ISession>) Whitebox.getInternalState(sessionPool, "queue")).size());
    }

    @Test
    public void testInsertRecords2() throws Exception {
        ConcurrentLinkedDeque<ISession> queue = new ConcurrentLinkedDeque<>();
        queue.add(session);
        Whitebox.setInternalState(sessionPool, "queue", queue);
        // 调用 insertRecords 方法
        List<String> deviceIds = Arrays.asList("device1", "device2");
        List<Long> timeList = Arrays.asList(1L, 2L);
        List<List<String>> measurementsList =
                Arrays.asList(
                        Arrays.asList("temperature", "humidity"), Arrays.asList("voltage", "current"));
        List<List<TSDataType>> typesList =
                Arrays.asList(
                        Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT),
                        Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE));
        List<List<Object>> valuesList =
                Arrays.asList(Arrays.asList(25.0f, 50.0f), Arrays.asList(220.0, 1.5));
        try {
            sessionPool.insertRecords(deviceIds, timeList, measurementsList, typesList, valuesList);
        }catch (IoTDBConnectionException e){
            assertTrue(e instanceof IoTDBConnectionException);
        }
    }
}