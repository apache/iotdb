package cn.edu.tsinghua.iotdb.jdbc;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import cn.edu.tsinghua.iotdb.jdbc.thrift.TSGetTimeZoneResp;
import cn.edu.tsinghua.iotdb.jdbc.thrift.TSIService;
import cn.edu.tsinghua.iotdb.jdbc.thrift.TSSetTimeZoneReq;
import cn.edu.tsinghua.iotdb.jdbc.thrift.TSSetTimeZoneResp;
import cn.edu.tsinghua.iotdb.jdbc.thrift.TS_Status;
import cn.edu.tsinghua.iotdb.jdbc.thrift.TS_StatusCode;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import org.apache.thrift.TException;


public class TsfileConnectionTest {
    @Mock
    private TSIService.Iface client;
    
    private TsfileConnection connection = new TsfileConnection();
    private TS_Status Status_SUCCESS = new TS_Status(TS_StatusCode.SUCCESS_STATUS);

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testSetTimeZone() throws TException, TsfileSQLException {
		String timeZone = "Asia/Shanghai";
		when(client.setTimeZone(any(TSSetTimeZoneReq.class))).thenReturn(new TSSetTimeZoneResp(Status_SUCCESS));
		connection.client = client;
		connection.setTimeZone(timeZone);
		assertEquals(connection.getTimeZone(), timeZone);
	}

	@Test
	public void testGetTimeZone() throws TsfileSQLException, TException {
	    String timeZone = "GMT+:08:00";
		when(client.getTimeZone()).thenReturn(new TSGetTimeZoneResp(Status_SUCCESS, timeZone));
		connection.client = client;
		assertEquals(connection.getTimeZone(), timeZone);
	}


}
