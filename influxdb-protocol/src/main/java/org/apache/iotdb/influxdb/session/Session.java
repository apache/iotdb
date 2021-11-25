package org.apache.iotdb.influxdb.session;

import org.apache.iotdb.protocol.influxdb.rpc.thrift.InfluxDBService;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.RpcUtils;

import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.lang.reflect.Proxy;
import java.time.ZoneId;

public class Session {

  private TTransport transport;
  private InfluxDBService.Iface client;
  private long sessionId;


  public Session() throws IoTDBConnectionException {
    RpcTransportFactory.setDefaultBufferCapacity(1024);
    RpcTransportFactory.setThriftMaxFrameSize(67108864);
    try {
      transport =
          RpcTransportFactory.INSTANCE.getTransport(
              // as there is a try-catch already, we do not need to use TSocket.wrap
              "127.0.0.1", 8086, 0);
      transport.open();
    } catch (TTransportException e) {
      throw new IoTDBConnectionException(e);
    }

    client = new InfluxDBService.Client(new TCompactProtocol(transport));
    client = RpcUtils.newSynchronizedClient(client);

    TSOpenSessionReq openReq = new TSOpenSessionReq();
    openReq.setUsername("root");
    openReq.setPassword("root");
    openReq.setZoneId(ZoneId.systemDefault().toString());
    openReq.setClient_protocol(TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3);

    try {
      TSOpenSessionResp openResp = client.openSession(openReq);

      sessionId = openResp.getSessionId();

    } catch (Exception e) {
      transport.close();
      throw new IoTDBConnectionException(e);
    }
    System.out.println(sessionId);
  }
}
