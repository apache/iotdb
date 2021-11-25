package org.apache.iotdb.rpc;

import org.apache.iotdb.protocol.influxdb.rpc.thrift.InfluxDBService;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.thrift.TException;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class InfluxDBSynchronizedHandler implements InvocationHandler {

    private final InfluxDBService.Iface client;

    public InfluxDBSynchronizedHandler(InfluxDBService.Iface client) {
        this.client = client;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        try {
            synchronized (client) {
                return method.invoke(client, args);
            }
        } catch (InvocationTargetException e) {
            // all IFace APIs throw TException
            if (e.getTargetException() instanceof TException) {
                throw e.getTargetException();
            } else {
                // should not happen
                throw new TException("Error in calling method " + method.getName(), e.getTargetException());
            }
        } catch (Exception e) {
            throw new TException("Error in calling method " + method.getName(), e);
        }
    }
}