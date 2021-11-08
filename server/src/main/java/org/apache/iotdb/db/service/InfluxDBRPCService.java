package org.apache.iotdb.db.service;

import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.runtime.RPCServiceException;
import org.apache.iotdb.db.service.thrift.ThriftService;
import org.apache.iotdb.db.service.thrift.ThriftServiceThread;
import org.apache.iotdb.db.service.thriftImpl.TSServiceImpl;
import org.apache.iotdb.service.rpc.thrift.TSIService;


public class InfluxDBRPCService extends ThriftService implements RPCServiceMBean {

    private TSServiceImpl impl;

    private InfluxDBRPCService () {}

    public static InfluxDBRPCService getInstance() {
        return InfluxDBRPCServiceHolder.INSTANCE;
    }

    @Override
    public int getRPCPort() {
        IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
        return config.getRpcPort();
    }

    @Override
    public ThriftService getImplementation() {
        return getInstance();
    }

    @Override
    public void initTProcessor()
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        impl =
                (TSServiceImpl)
                        Class.forName(IoTDBDescriptor.getInstance().getConfig().getRpcImplClassName())
                                .newInstance();
        processor = new TSIService.Processor<>(impl);
    }

    @Override
    public void initThriftServiceThread() throws IllegalAccessException {
        IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
        try {
            thriftServiceThread =
                    new ThriftServiceThread(
                            processor,
                            getID().getName(),
                            ThreadName.RPC_CLIENT.getName(),
                            config.getRpcAddress(),
                            config.getRpcPort(),
                            config.getRpcMaxConcurrentClientNum(),
                            config.getThriftServerAwaitTimeForStopService(),
                            new RPCServiceThriftHandler(impl),
                            IoTDBDescriptor.getInstance().getConfig().isRpcThriftCompressionEnable());
        } catch (RPCServiceException e) {
            throw new IllegalAccessException(e.getMessage());
        }
        thriftServiceThread.setName(ThreadName.RPC_SERVICE.getName());
    }

    @Override
    public String getBindIP() {
        return IoTDBDescriptor.getInstance().getConfig().getRpcAddress();
    }

    @Override
    public int getBindPort() {
        return IoTDBDescriptor.getInstance().getConfig().getRpcPort();
    }

    @Override
    public ServiceType getID() {
        return ServiceType.RPC_SERVICE;
    }

    private static class InfluxDBRPCServiceHolder {

        private static final InfluxDBRPCService INSTANCE = new InfluxDBRPCService();

        private InfluxDBRPCServiceHolder() {}
    }
}
