package cn.edu.tsinghua.iotdb.jdbc;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import cn.edu.tsinghua.service.rpc.thrift.ServerProperties;
import cn.edu.tsinghua.service.rpc.thrift.TSGetTimeZoneResp;
import cn.edu.tsinghua.service.rpc.thrift.TSIService;
import cn.edu.tsinghua.service.rpc.thrift.TSSetTimeZoneReq;
import cn.edu.tsinghua.service.rpc.thrift.TSSetTimeZoneResp;
import cn.edu.tsinghua.service.rpc.thrift.TS_Status;
import cn.edu.tsinghua.service.rpc.thrift.TS_StatusCode;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

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

	@Test
	public void testGetServerProperties() throws TsfileSQLException, TException {
		final String version = "v0.1";
		@SuppressWarnings("serial")
		final List<String> supportedAggregationTime = new ArrayList<String>() {{
		    add("max_time");
		    add("min_time");
		}};
		when(client.getProperties()).thenReturn(new ServerProperties(version, supportedAggregationTime));
		connection.client = client;
		assertEquals(connection.getServerProperties().getVersion(), version);
		for(int i = 0; i < supportedAggregationTime.size();i++) {
			assertEquals(connection.getServerProperties().getSupportedTimeAggregationOperations().get(i), supportedAggregationTime.get(i));
		}
	}
}
