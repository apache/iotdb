package cn.edu.thu.tsfiledb.service;

import java.io.IOException;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSIService;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSIService.Processor;

/**
 * A server to handle jdbc request from client.
 */
public class JDBCServer implements JDBCServerMBean {
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCServer.class);
    private Thread jdbcServerThread;
    private boolean isStart;

//    private Factory protocolFactory;
    private TSServiceImpl serviceImpl;
    private Processor<TSIService.Iface> processor;
    private TNonblockingServerSocket serverSocket;
//    private TThreadPoolServer.Args poolArgs;
    private TServer server;

    public JDBCServer() throws TTransportException {
        isStart = false;
    }

    @Override
    public synchronized void startServer() {
        if (isStart) {
            LOGGER.info("TsFileDB: jdbc server has been already running now");
            return;
        }
        LOGGER.info("TsFileDB: start jdbc server...");

        try {
            jdbcServerThread = new Thread(new JDBCServerThread());
        } catch (IOException e) {
            LOGGER.error("TsFileDB Server: failed to start jdbc server. {}", e.getMessage());
            return;
        }
        jdbcServerThread.start();

        LOGGER.info("TsFileDB: start jdbc server successfully, listening on port {}",TsfileDBDescriptor.getInstance().getConfig().rpcPort);
        isStart = true;
    }

    @Override
    public synchronized void restartServer() {
        stopServer();
        startServer();
    }

    @Override
    public synchronized void stopServer() {
        if (!isStart) {
            LOGGER.info("TsFileDB: jdbc server isn't running now");
            return;

        }
        LOGGER.info("TsFileDB: closing jdbc server...");
        close();
        LOGGER.info("TsFileDB: close jdbc server successfully");
    }

    private synchronized void close() {
        if (server != null) {
            server.stop();
            server = null;
        }

        if (serverSocket != null) {
            serverSocket.close();
            serverSocket = null;
        }
        isStart = false;
    }

    private class JDBCServerThread implements Runnable {

        public JDBCServerThread() throws IOException {
            serviceImpl = new TSServiceImpl();
            processor = new TSIService.Processor<TSIService.Iface>(serviceImpl);
        }

        @Override
        public void run() {
            try {
                serverSocket = new TNonblockingServerSocket(TsfileDBDescriptor.getInstance().getConfig().rpcPort);
                TThreadedSelectorServer.Args serverParams=new TThreadedSelectorServer.Args(serverSocket);
                serverParams.protocolFactory(new TBinaryProtocol.Factory());
                serverParams.processor(processor);  
                server = new TNonblockingServer(serverParams); 
                server.setServerEventHandler(new JDBCServerEventHandler(serviceImpl));
                server.serve();
            } catch (TTransportException e) {
                LOGGER.error("TsFileDB: failed to start jdbc server, because ", e);
            } catch (Exception e) {
                LOGGER.error("TsFileDB: jdbc server exit, because ", e);
            } finally {
                close();
                LOGGER.info("TsFileDB: close TThreadPoolServer and TServerSocket for jdbc server");
            }
        }
    }

    public static void main(String[] args) throws TTransportException, InterruptedException {
        JDBCServer server = new JDBCServer();
        server.startServer();
    }
}
