package org.apache.iotdb.jdbc;

import org.apache.iotdb.service.rpc.thrift.*;
import org.apache.iotdb.service.rpc.thrift.TSIService.Iface;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZoneId;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IoTDBPrepareStatementTest {
	private ZoneId zoneId = ZoneId.systemDefault(); 
	@Mock
	private IoTDBConnection connection;
	
	@Mock
	private Iface client;
	
	@Mock
	private TS_SessionHandle sessHandle;
	
	@Mock
	TSExecuteStatementResp execStatementResp;
	
	@Mock
	TSGetOperationStatusResp getOperationStatusResp;
	
	private TS_Status Status_SUCCESS = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
	
	@Mock
	private TSOperationHandle tOperationHandle;

	@Before
	public void before() throws Exception {
		MockitoAnnotations.initMocks(this);
		when(execStatementResp.getStatus()).thenReturn(Status_SUCCESS);
		when(execStatementResp.getOperationHandle()).thenReturn(tOperationHandle);

		when(getOperationStatusResp.getStatus()).thenReturn(Status_SUCCESS);
		when(client.executeStatement(any(TSExecuteStatementReq.class))).thenReturn(execStatementResp);
	}

	@SuppressWarnings("resource")
	@Test
	public void testNonParameterized() throws Exception {
		String sql = "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 24 and time > 2017-11-1 0:13:00";
		IoTDBPrepareStatement ps = new IoTDBPrepareStatement(connection, client, sessHandle, sql, zoneId);
		ps.execute();

		ArgumentCaptor<TSExecuteStatementReq> argument = ArgumentCaptor.forClass(TSExecuteStatementReq.class);
		verify(client).executeStatement(argument.capture());
		assertEquals("SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 24 and time > 2017-11-1 0:13:00", argument.getValue().getStatement());
	}

	@SuppressWarnings("resource")
	@Test
	public void unusedArgument() throws SQLException {
		String sql = "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 24 and time > 2017-11-1 0:13:00";
		IoTDBPrepareStatement ps = new IoTDBPrepareStatement(connection, client, sessHandle, sql, zoneId);
		ps.setString(1, "123");
		ps.execute();
	}

	@SuppressWarnings("resource")
	@Test(expected = SQLException.class)
	public void unsetArgument() throws SQLException {
		String sql = "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 24 and time > ?";
		IoTDBPrepareStatement ps = new IoTDBPrepareStatement(connection, client, sessHandle, sql, zoneId);
		ps.execute();
	}

	@SuppressWarnings("resource")
	@Test
	public void oneIntArgument() throws Exception {
		String sql = "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < ? and time > 2017-11-1 0:13:00";
		IoTDBPrepareStatement ps = new IoTDBPrepareStatement(connection, client, sessHandle, sql, zoneId);
		ps.setInt(1, 123);
		ps.execute();
		ArgumentCaptor<TSExecuteStatementReq> argument = ArgumentCaptor.forClass(TSExecuteStatementReq.class);
		verify(client).executeStatement(argument.capture());
		assertEquals("SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 123 and time > 2017-11-1 0:13:00", argument.getValue().getStatement());
	}
	
	@SuppressWarnings("resource")
	@Test
	public void oneLongArgument() throws Exception {
		String sql = "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < ? and time > 2017-11-1 0:13:00";
		IoTDBPrepareStatement ps = new IoTDBPrepareStatement(connection, client, sessHandle, sql, zoneId);
		ps.setLong(1, 123);
		ps.execute();
		ArgumentCaptor<TSExecuteStatementReq> argument = ArgumentCaptor.forClass(TSExecuteStatementReq.class);
		verify(client).executeStatement(argument.capture());
		assertEquals("SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 123 and time > 2017-11-1 0:13:00", argument.getValue().getStatement());
	}
	
	@SuppressWarnings("resource")
	@Test
	public void oneFloatArgument() throws Exception {
		String sql = "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < ? and time > 2017-11-1 0:13:00";
		IoTDBPrepareStatement ps = new IoTDBPrepareStatement(connection, client, sessHandle, sql, zoneId);
		ps.setFloat(1, 123.133f);
		ps.execute();
		ArgumentCaptor<TSExecuteStatementReq> argument = ArgumentCaptor.forClass(TSExecuteStatementReq.class);
		verify(client).executeStatement(argument.capture());
		assertEquals("SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 123.133 and time > 2017-11-1 0:13:00", argument.getValue().getStatement());
	}
	
	@SuppressWarnings("resource")
	@Test
	public void oneDoubleArgument() throws Exception {
		String sql = "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < ? and time > 2017-11-1 0:13:00";
		IoTDBPrepareStatement ps = new IoTDBPrepareStatement(connection, client, sessHandle, sql, zoneId);
		ps.setDouble(1, 123.456);
		ps.execute();
		ArgumentCaptor<TSExecuteStatementReq> argument = ArgumentCaptor.forClass(TSExecuteStatementReq.class);
		verify(client).executeStatement(argument.capture());
		assertEquals("SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 123.456 and time > 2017-11-1 0:13:00", argument.getValue().getStatement());
	}
	
	@SuppressWarnings("resource")
	@Test
	public void oneBooleanArgument() throws Exception {
		String sql = "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < ? and time > 2017-11-1 0:13:00";
		IoTDBPrepareStatement ps = new IoTDBPrepareStatement(connection, client, sessHandle, sql, zoneId);
		ps.setBoolean(1, false);
		ps.execute();
		ArgumentCaptor<TSExecuteStatementReq> argument = ArgumentCaptor.forClass(TSExecuteStatementReq.class);
		verify(client).executeStatement(argument.capture());
		assertEquals("SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < false and time > 2017-11-1 0:13:00", argument.getValue().getStatement());
	}
	
	@SuppressWarnings("resource")
	@Test
	public void oneStringArgument() throws Exception {
		String sql = "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < ? and time > 2017-11-1 0:13:00";
		IoTDBPrepareStatement ps = new IoTDBPrepareStatement(connection, client, sessHandle, sql, zoneId);
		ps.setString(1, "abcde");
		ps.execute();
		ArgumentCaptor<TSExecuteStatementReq> argument = ArgumentCaptor.forClass(TSExecuteStatementReq.class);
		verify(client).executeStatement(argument.capture());
		assertEquals("SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 'abcde' and time > 2017-11-1 0:13:00", argument.getValue().getStatement());
	}
	
	@SuppressWarnings("resource")
	@Test
	public void oneTimeLongArgument() throws Exception {
		String sql = "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE time > ?";
		IoTDBPrepareStatement ps = new IoTDBPrepareStatement(connection, client, sessHandle, sql, zoneId);
		ps.setLong(1, 1233);
		ps.execute();
		ArgumentCaptor<TSExecuteStatementReq> argument = ArgumentCaptor.forClass(TSExecuteStatementReq.class);
		verify(client).executeStatement(argument.capture());
		assertEquals("SELECT status, temperature FROM root.ln.wf01.wt01 WHERE time > 1233", argument.getValue().getStatement());
	}

	@SuppressWarnings("resource")
	@Test
	public void oneTimeTimestampArgument() throws Exception {
		String sql = "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE time > ?";
		IoTDBPrepareStatement ps = new IoTDBPrepareStatement(connection, client, sessHandle, sql, zoneId);
		ps.setTimestamp(1, Timestamp.valueOf("2017-11-01 00:13:00"));
		ps.execute();
		ArgumentCaptor<TSExecuteStatementReq> argument = ArgumentCaptor.forClass(TSExecuteStatementReq.class);
		verify(client).executeStatement(argument.capture());
		assertEquals("SELECT status, temperature FROM root.ln.wf01.wt01 WHERE time > 2017-11-01T00:13:00", argument.getValue().getStatement());
	}
	
	@SuppressWarnings("resource")
	@Test
	public void escapingOfStringArgument() throws Exception {
		String sql = "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE status = '134' and temperature = ?";
		IoTDBPrepareStatement ps = new IoTDBPrepareStatement(connection, client, sessHandle, sql, zoneId);
		ps.setLong(1, 1333);
		ps.execute();
		
		ArgumentCaptor<TSExecuteStatementReq> argument = ArgumentCaptor.forClass(TSExecuteStatementReq.class);
		verify(client).executeStatement(argument.capture());
		assertEquals("SELECT status, temperature FROM root.ln.wf01.wt01 WHERE status = '134' and temperature = 1333", argument.getValue().getStatement());
	}
	
	@SuppressWarnings("resource")
	@Test
	public void pastingIntoEscapedQuery() throws Exception {
		String sql = "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE status = '\\044e' || temperature = ?";

		IoTDBPrepareStatement ps = new IoTDBPrepareStatement(connection, client, sessHandle, sql, zoneId);
		ps.setDouble(1, -1323.0);
		ps.execute();
		
		ArgumentCaptor<TSExecuteStatementReq> argument = ArgumentCaptor.forClass(TSExecuteStatementReq.class);
		verify(client).executeStatement(argument.capture());
		assertEquals("SELECT status, temperature FROM root.ln.wf01.wt01 WHERE status = '\\044e' || temperature = -1323.0", argument.getValue().getStatement());
	}
	
	@SuppressWarnings("resource")
	@Test
	public void testInsertStatement1() throws Exception{
		String sql = "INSERT INTO root.ln.wf01.wt01(timestamp,a,b,c,d,e,f) VALUES(?,?,?,?,?,?,?)";

		IoTDBPrepareStatement ps = new IoTDBPrepareStatement(connection, client, sessHandle, sql, zoneId);
		ps.setLong(1, 12324);
		ps.setBoolean(2, false);
		ps.setInt(3, 123);
		ps.setLong(4, 123234345);
		ps.setFloat(5, 123.423f);
		ps.setDouble(6, -1323.0);
		ps.setString(7, "abc");
		ps.execute();
		
		ArgumentCaptor<TSExecuteStatementReq> argument = ArgumentCaptor.forClass(TSExecuteStatementReq.class);
		verify(client).executeStatement(argument.capture());
		assertEquals("INSERT INTO root.ln.wf01.wt01(timestamp,a,b,c,d,e,f) VALUES(12324,false,123,123234345,123.423,-1323.0,'abc')", argument.getValue().getStatement());
	}
	
	@SuppressWarnings("resource")
	@Test
	public void testInsertStatement2() throws Exception{
		String sql = "INSERT INTO root.ln.wf01.wt01(timestamp,a,b,c,d,e,f) VALUES(?,?,?,?,?,?,?)";

		IoTDBPrepareStatement ps = new IoTDBPrepareStatement(connection, client, sessHandle, sql, zoneId);
		ps.setTimestamp(1, Timestamp.valueOf("2017-11-01 00:13:00"));
		ps.setBoolean(2, false);
		ps.setInt(3, 123);
		ps.setLong(4, 123234345);
		ps.setFloat(5, 123.423f);
		ps.setDouble(6, -1323.0);
		ps.setString(7, "abc");
		ps.execute();
		
		ArgumentCaptor<TSExecuteStatementReq> argument = ArgumentCaptor.forClass(TSExecuteStatementReq.class);
		verify(client).executeStatement(argument.capture());
		assertEquals("INSERT INTO root.ln.wf01.wt01(timestamp,a,b,c,d,e,f) VALUES(2017-11-01T00:13:00,false,123,123234345,123.423,-1323.0,'abc')", argument.getValue().getStatement());
	}
}
