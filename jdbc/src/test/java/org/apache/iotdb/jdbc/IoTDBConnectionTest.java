package org.apache.iotdb.jdbc;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.iotdb.service.rpc.thrift.ServerProperties;
import org.apache.iotdb.service.rpc.thrift.TSGetTimeZoneResp;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneReq;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneResp;
import org.apache.iotdb.service.rpc.thrift.TS_Status;
import org.apache.iotdb.service.rpc.thrift.TS_StatusCode;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;


public class IoTDBConnectionTest {
    @Mock
    private TSIService.Iface client;
    
    private IoTDBConnection connection = new IoTDBConnection();
    private TS_Status Status_SUCCESS = new TS_Status(TS_StatusCode.SUCCESS_STATUS);

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testSetTimeZone() throws TException, IoTDBSQLException {
		String timeZone = "Asia/Shanghai";
		when(client.setTimeZone(any(TSSetTimeZoneReq.class))).thenReturn(new TSSetTimeZoneResp(Status_SUCCESS));
		connection.client = client;
		connection.setTimeZone(timeZone);
		assertEquals(connection.getTimeZone(), timeZone);
	}

	@Test
	public void testGetTimeZone() throws IoTDBSQLException, TException {
	    String timeZone = "GMT+:08:00";
		when(client.getTimeZone()).thenReturn(new TSGetTimeZoneResp(Status_SUCCESS, timeZone));
		connection.client = client;
		assertEquals(connection.getTimeZone(), timeZone);
	}

	@Test
	public void testGetServerProperties() throws IoTDBSQLException, TException {
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
