package cn.edu.thu.tsfiledb.service;

import java.io.IOException;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.auth.dao.DBDao;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.engine.exception.FileNodeManagerException;
import cn.edu.thu.tsfiledb.engine.exception.LRUManagerException;
import cn.edu.thu.tsfiledb.engine.filenode.FileNodeManager;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSIService;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSIService.Processor;

/**
 * A server to handle jdbc request from client.
 */
public class JDBCServer implements JDBCServerMBean {
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCServer.class);
    private DBDao dBdao;
    private Thread jdbcServerThread;
    private boolean isStart;

    private Factory protocolFactory;
    private Processor<TSIService.Iface> processor;
    private TServerSocket serverTransport;
    private TThreadPoolServer.Args poolArgs;
    private TServer poolServer;

    public JDBCServer() throws TTransportException {
        isStart = false;
    }

    @Override
    public synchronized void startServer() {
        if (isStart) {
            LOGGER.info("TsFileDB Server: server has been already running now");
            return;
        }
        LOGGER.info("TsFileDB Server: start server...");
        dBdao = new DBDao();
        dBdao.open();
        FileNodeManager.getInstance().managerRecovery();
        try {
            jdbcServerThread = new Thread(new JDBCServerThread());
        } catch (IOException e) {
            LOGGER.error("TsFileDB Server: failed to start server. {}", e.getMessage());
            if (dBdao != null) {
                dBdao.close();
            }
            return;
        }
        jdbcServerThread.start();


        LOGGER.info("TsFileDB Server: start server successfully");
        LOGGER.info("Listening on port: {}", TsfileDBDescriptor.getInstance().getConfig().rpcPort);
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
            LOGGER.info("TsFileDB Server: server isn't running now");
            return;

        }

        LOGGER.info("TsFileDB Server: closing jdbc server...");

        if (dBdao != null) {
            dBdao.close();
            dBdao = null;
        }

        try {
            FileNodeManager.getInstance().close();
        } catch (LRUManagerException e) {
            e.printStackTrace();
        }

        close();
        LOGGER.info("TsFileDB Server: close server successfully");
    }

    private synchronized void close() {
        if (poolServer != null) {
            poolServer.stop();
            poolServer = null;
        }

        if (serverTransport != null) {
            serverTransport.close();
            serverTransport = null;
        }
        isStart = false;
    }

    private class JDBCServerThread implements Runnable {

        public JDBCServerThread() throws IOException {
            protocolFactory = new TBinaryProtocol.Factory();
            processor = new TSIService.Processor<TSIService.Iface>(new TSServiceImpl());
        }

        @Override
        public void run() {
            try {
                serverTransport = new TServerSocket(TsfileDBDescriptor.getInstance().getConfig().rpcPort);
                poolArgs = new TThreadPoolServer.Args(serverTransport);
                poolArgs.processor(processor);
                poolArgs.protocolFactory(protocolFactory);
                poolServer = new TThreadPoolServer(poolArgs);
                poolServer.serve();
            } catch (TTransportException e) {
                LOGGER.error("TsFileDB Server: failed to start server, because ", e);
            } catch (Exception e) {
                LOGGER.error("TsFileDB Server: server exit, because ", e);
            } finally {
                close();
                LOGGER.info("TsFileDB Server: close TThreadPoolServer and TServerSocket");
            }
        }
    }

    public static void main(String[] args) throws TTransportException, InterruptedException {
        JDBCServer server = new JDBCServer();
        server.startServer();
    }

    @Override
    public synchronized void mergeAll() {
        LOGGER.info("TsFileDB Server: start merging...");
        try {
            FileNodeManager.getInstance().mergeAll();
        } catch (FileNodeManagerException e) {
            e.printStackTrace();
        }
        LOGGER.info("TsFileDB Server: finish merge operation.");
    }
}
