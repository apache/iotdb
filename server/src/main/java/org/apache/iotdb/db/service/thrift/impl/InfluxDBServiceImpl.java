package org.apache.iotdb.db.service.thrift.impl;

import org.apache.iotdb.protocol.influxdb.rpc.thrift.*;
import org.apache.thrift.TException;

public class InfluxDBServiceImpl implements InfluxDBService.Iface{
    @Override
    public TSOpenSessionResp openSession(TSOpenSessionReq req) throws TException {
        TSOpenSessionResp tsOpenSessionResp = new TSOpenSessionResp();
        tsOpenSessionResp.sessionId=1;
        return tsOpenSessionResp;
    }

    @Override
    public TSStatus closeSession(TSCloseSessionReq req) throws TException {
        return null;
    }

    @Override
    public TSStatus writePoints(TSWritePointsReq req) throws TException {
        return null;
    }

    public void handleClientExit() {
    }
}
