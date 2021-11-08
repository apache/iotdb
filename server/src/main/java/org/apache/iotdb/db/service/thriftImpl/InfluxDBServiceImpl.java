package org.apache.iotdb.db.service.thriftImpl;

import org.apache.iotdb.protocol.influxdb.rpc.thrift.*;
import org.apache.thrift.TException;

public class InfluxDBServiceImpl implements InfluxDBService.Iface{
    @Override
    public TSOpenSessionResp openSession(TSOpenSessionReq req) throws TException {
        return null;
    }

    @Override
    public TSStatus closeSession(TSCloseSessionReq req) throws TException {
        return null;
    }

    @Override
    public TSStatus writePoints(TSWritePointsReq req) throws TException {
        return null;
    }
}
