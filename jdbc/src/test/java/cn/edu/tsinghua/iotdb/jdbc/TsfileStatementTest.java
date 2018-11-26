package cn.edu.tsinghua.iotdb.jdbc;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.any;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import cn.edu.tsinghua.service.rpc.thrift.TSFetchMetadataReq;
import cn.edu.tsinghua.service.rpc.thrift.TSFetchMetadataResp;
import cn.edu.tsinghua.service.rpc.thrift.TS_SessionHandle;
import cn.edu.tsinghua.service.rpc.thrift.TS_Status;
import cn.edu.tsinghua.service.rpc.thrift.TS_StatusCode;
import cn.edu.tsinghua.service.rpc.thrift.TSIService.Iface;

public class TsfileStatementTest {
	@Mock
	private TsfileConnection connection;
	
	@Mock
	private Iface client;
	
	@Mock
	private TS_SessionHandle sessHandle;
	
    @Mock
    private TSFetchMetadataResp fetchMetadataResp;

    private TS_Status Status_SUCCESS = new TS_Status(TS_StatusCode.SUCCESS_STATUS);

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		when(connection.getMetaData()).thenReturn(new TsfileDatabaseMetadata(connection, client));
		when(connection.isClosed()).thenReturn(false);
        when(client.fetchMetadata(any(TSFetchMetadataReq.class))).thenReturn(fetchMetadataResp);
        when(fetchMetadataResp.getStatus()).thenReturn(Status_SUCCESS);
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@SuppressWarnings("resource")
	@Test(expected = SQLException.class)
	public void testExecuteSQL1() throws SQLException {
		TsfileStatement stmt = new TsfileStatement(connection, client, sessHandle);
		stmt.execute("show timeseries");
	}
	
	@SuppressWarnings({ "resource", "serial" })
	@Test
	public void testExecuteSQL2() throws SQLException, TException {
		TsfileStatement stmt = new TsfileStatement(connection, client, sessHandle);
        List<List<String>> tslist = new ArrayList<>();
        tslist.add(new ArrayList<String>(4) {{
            add("root.vehicle.d0.s0");
            add("root.vehicle");
            add("INT32");
            add("RLE");
        }});
        tslist.add(new ArrayList<String>(4) {{
            add("root.vehicle.d0.s1");
            add("root.vehicle");
            add("INT64");
            add("RLE");
        }});
        tslist.add(new ArrayList<String>(4) {{
            add("root.vehicle.d0.s2");
            add("root.vehicle");
            add("FLOAT");
            add("RLE");
        }});
        String standard = "Timeseries,Storage Group,DataType,Encoding,\n" +
                "root.vehicle.d0.s0,root.vehicle,INT32,RLE,\n" +
                "root.vehicle.d0.s1,root.vehicle,INT64,RLE,\n" +
                "root.vehicle.d0.s2,root.vehicle,FLOAT,RLE,\n";
        when(fetchMetadataResp.getShowTimeseriesList()).thenReturn(tslist);
        boolean res = stmt.execute("show timeseries root");
		assertEquals(res, true);
        try {
        	ResultSet resultSet = stmt.getResultSet();
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int colCount = resultSetMetaData.getColumnCount();
            StringBuilder resultStr = new StringBuilder();
            for (int i = 1; i < colCount + 1; i++) {
                resultStr.append(resultSetMetaData.getColumnName(i)).append(",");
            }
            resultStr.append("\n");
            while (resultSet.next()) {
                for (int i = 1; i <= colCount; i++) {
                    resultStr.append(resultSet.getString(i)).append(",");
                }
                resultStr.append("\n");
            }
            Assert.assertEquals(resultStr.toString(), standard);
        } catch (SQLException e) {
            System.out.println(e);
        }
	}
	
	@SuppressWarnings({ "resource"})
	@Test
	public void testExecuteSQL3() throws SQLException, TException {
		TsfileStatement stmt = new TsfileStatement(connection, client, sessHandle);
		Set<String> sgSet = new HashSet<>();
        sgSet.add("root.vehicle");
        when(fetchMetadataResp.getShowStorageGroups()).thenReturn(sgSet);
        String standard = "Storage Group,\nroot.vehicle,\n";
        boolean res = stmt.execute("show storage group");
		assertEquals(res, true);
        try {
        	ResultSet resultSet = stmt.getResultSet();
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int colCount = resultSetMetaData.getColumnCount();
            StringBuilder resultStr = new StringBuilder();
            for (int i = 1; i < colCount + 1; i++) {
                resultStr.append(resultSetMetaData.getColumnName(i)).append(",");
            }
            resultStr.append("\n");
            while (resultSet.next()) {
                for (int i = 1; i <= colCount; i++) {
                    resultStr.append(resultSet.getString(i)).append(",");
                }
                resultStr.append("\n");
            }
            Assert.assertEquals(resultStr.toString(), standard);
        } catch (SQLException e) {
            System.out.println(e);
        }
	}
	
	@SuppressWarnings("resource")
	@Test
	public void testSetFetchSize1() throws SQLException {
		TsfileStatement stmt = new TsfileStatement(connection, client, sessHandle);
	    stmt.setFetchSize(123);
	    assertEquals(123, stmt.getFetchSize());
	}

	@SuppressWarnings("resource")
	@Test
	public void testSetFetchSize2() throws SQLException {
		TsfileStatement stmt = new TsfileStatement(connection, client, sessHandle);
	    int initial = stmt.getFetchSize();
	    stmt.setFetchSize(0);
	    assertEquals(initial, stmt.getFetchSize());
	}
	
	@SuppressWarnings("resource")
	@Test
	public void testSetFetchSize3() throws SQLException {
		final int fetchSize = 10000;
		TsfileStatement stmt = new TsfileStatement(connection, client, sessHandle, fetchSize);
	    assertEquals(fetchSize, stmt.getFetchSize());
	}

	@SuppressWarnings("resource")
	@Test(expected = SQLException.class)
	public void testSetFetchSize4() throws SQLException {
		TsfileStatement stmt = new TsfileStatement(connection, client, sessHandle);
	    stmt.setFetchSize(-1);
	}

	@SuppressWarnings("resource")
	@Test
	public void testSetMaxRows1() throws SQLException {
		TsfileStatement stmt = new TsfileStatement(connection, client, sessHandle);
	    stmt.setMaxRows(123);
	    assertEquals(123, stmt.getMaxRows());
	}
	
	@SuppressWarnings("resource")
	@Test(expected = SQLException.class)
	public void testSetMaxRows2() throws SQLException {
		TsfileStatement stmt = new TsfileStatement(connection, client, sessHandle);
		stmt.setMaxRows(-1);
	}
}
