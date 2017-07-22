package cn.edu.thu.tsfiledb.service;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.sql.SQLException;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfiledb.qp.QueryProcessor;
import cn.edu.thu.tsfiledb.qp.executor.OverflowQPExecutor;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;
import cn.edu.thu.tsfiledb.sys.writelog.WriteLogManager;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.auth.dao.DBDao;
import cn.edu.thu.tsfiledb.auth.dao.DBDaoInitException;
import cn.edu.thu.tsfiledb.conf.TsFileDBConstant;
import cn.edu.thu.tsfiledb.engine.exception.FileNodeManagerException;
import cn.edu.thu.tsfiledb.engine.filenode.FileNodeManager;
import cn.edu.thu.tsfiledb.exception.StartupException;

public class Daemon {

    private static final Logger LOGGER = LoggerFactory.getLogger(Daemon.class);
    static final Daemon instance = new Daemon();
    private JMXConnectorServer jmxServer;
    private MBeanServer mbs;
    private DBDao dBdao;
    private JDBCServerMBean jdbcMBean;

    public Daemon() {
        mbs = ManagementFactory.getPlatformMBeanServer();
    }

    public void active() {
        StartupChecks checks = new StartupChecks().withDefaultTest();
        try {
            checks.verify();
        } catch (StartupException e) {
            LOGGER.error("TsFileDB: failed to start because of some check fail. {}", e.getMessage());
            return;
        }
        try {
            setUp();
        } catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException
                | NotCompliantMBeanException | TTransportException | IOException e) {
            LOGGER.error("TsFileDB: failed to start because: {}", e.getMessage());
        }
    }

    private void setUp() throws MalformedObjectNameException, InstanceAlreadyExistsException,
            MBeanRegistrationException, NotCompliantMBeanException, TTransportException, IOException {
    	try {
			initDBDao();
		} catch (ClassNotFoundException | SQLException | DBDaoInitException e) {
			LOGGER.error("Fail to start TsFileDB!");
			return;
		}

        initFileNodeManager();

        systemDataRecovery();

        maybeInitJmx();
        registJDBCServer();

        startCloseAndMergeServer();
    }

    private void maybeInitJmx() {
        if (System.getProperty(TsFileDBConstant.REMOTE_JMX_PORT_NAME) != null) {
            LOGGER.warn("JMX settings in tsfile-env.sh have been bypassed as the JMX connector server is "
                    + "already initialized. Please refer to tsfile-env.sh for JMX configuration info");
            return;
        }
        System.setProperty(TsFileDBConstant.SERVER_RMI_ID, "true");
        boolean localOnly = false;
        String jmxPort = System.getProperty(TsFileDBConstant.TSFILEDB_REMOTE_JMX_PORT_NAME);

        if (jmxPort == null) {
            localOnly = true;
            jmxPort = System.getProperty(TsFileDBConstant.TSFILEDB_LOCAL_JMX_PORT_NAME);
        }

        if (jmxPort == null)
            return;

        try {
            jmxServer = JMXServerUtils.createJMXServer(Integer.parseInt(jmxPort), localOnly);
            if (jmxServer == null)
                return;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void registJDBCServer() throws TTransportException, MalformedObjectNameException,
            InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
        jdbcMBean = new JDBCServer();
        jdbcMBean.startServer();
        ObjectName mBeanName = new ObjectName("JDBCServer", "type", "JDBCServer");
        mbs.registerMBean(jdbcMBean, mBeanName);
    }

    private void initDBDao() throws ClassNotFoundException, SQLException, DBDaoInitException {
        dBdao = new DBDao();
        dBdao.open();
    }

    private void initFileNodeManager() {
        FileNodeManager.getInstance().managerRecovery();
    }

    /**
     * Recover data using system log.
     *
     * @throws IOException
     */
    private void systemDataRecovery() throws IOException {
        LOGGER.info("TsFileDB Server: start checking write log...");
        QueryProcessor processor = new QueryProcessor(new OverflowQPExecutor());
        WriteLogManager writeLogManager = WriteLogManager.getInstance();
        writeLogManager.recovery();
        long cnt = 0L;
        PhysicalPlan plan;
        WriteLogManager.isRecovering = true;
        while ((plan = writeLogManager.getPhysicalPlan()) != null) {
            try {
                processor.getExecutor().processNonQuery(plan);
                cnt++;
            } catch (ProcessorException e) {
                e.printStackTrace();
                throw new IOException("Error in recovery from write log");
            }
        }
        WriteLogManager.isRecovering = false;
        LOGGER.info("TsFileDB Server: Done. Recover operation count {}", cnt);
    }

    /**
     * start the close and merge server
     */
    private void startCloseAndMergeServer() {
        // close and merge regularly
        CloseMergeServer.getInstance().startServer();
    }

    public void stop() throws FileNodeManagerException, IOException {
        if (dBdao != null) {
            dBdao.close();
        }

        FileNodeManager.getInstance().closeAll();

        if (jdbcMBean != null) {
            jdbcMBean.stopServer();
        }

        if (jmxServer != null) {
            jmxServer.stop();
        }
        CloseMergeServer.getInstance().closeServer();
        ;

    }

    public static void main(String[] args) {
        Daemon daemon = new Daemon();
        daemon.active();
    }

}
