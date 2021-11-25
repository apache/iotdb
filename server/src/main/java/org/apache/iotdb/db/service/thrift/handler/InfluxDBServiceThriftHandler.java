package org.apache.iotdb.db.service.thrift.handler;

import org.apache.iotdb.db.service.thrift.impl.InfluxDBServiceImpl;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TTransport;

public class InfluxDBServiceThriftHandler implements TServerEventHandler {
    private final InfluxDBServiceImpl influxDBServiceImpl;
    public InfluxDBServiceThriftHandler(InfluxDBServiceImpl influxDBServiceImpl){
        this.influxDBServiceImpl=influxDBServiceImpl;
    }
    @Override
    public void preServe() {
        //nothing

    }

    @Override
    public ServerContext createContext(TProtocol tProtocol, TProtocol tProtocol1) {
        //nothing
        return null;
    }

    @Override
    public void deleteContext(ServerContext serverContext, TProtocol tProtocol, TProtocol tProtocol1) {
        //release resources.
        influxDBServiceImpl.handleClientExit();
    }

    @Override
    public void processContext(ServerContext serverContext, TTransport tTransport, TTransport tTransport1) {
        //nothing
    }
}
