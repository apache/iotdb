package cn.edu.tsinghua.iotdb.service;

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

import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.auth.dao.DBDao;
import cn.edu.tsinghua.iotdb.auth.dao.DBDaoInitException;
import cn.edu.tsinghua.iotdb.conf.TsFileDBConstant;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.StartupException;
import cn.edu.tsinghua.iotdb.qp.physical.crud.DeletePlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.InsertPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.UpdatePlan;
import cn.edu.tsinghua.iotdb.sys.writelog.WriteLogManager;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;

public class IoTDB {

    private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB.class);
    static final IoTDB instance = new IoTDB();
    private JMXConnectorServer jmxServer;
    private MBeanServer mbs;
    private DBDao dBdao;
    private JDBCServerMBean jdbcMBean;
    private MonitorMBean monitorMBean;

    public IoTDB() {
        mbs = ManagementFactory.getPlatformMBeanServer();
    }

    public void active() {
        StartupChecks checks = new StartupChecks().withDefaultTest();
        try {
            checks.verify();
        } catch (StartupException e) {
            LOGGER.error("{}: failed to start because of some check fail. {}", TsFileDBConstant.GLOBAL_DB_NAME, e.getMessage());
            return;
        }
        try {
            setUp();
        } catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException
                | NotCompliantMBeanException | TTransportException | IOException e) {
            LOGGER.error("{}: failed to start because: {}",TsFileDBConstant.GLOBAL_DB_NAME, e.getMessage());
        } catch (FileNodeManagerException e) {
            e.printStackTrace();
        } catch (PathErrorException e) {
            e.printStackTrace();
        }
    }

    private void setUp() throws MalformedObjectNameException, InstanceAlreadyExistsException,
            MBeanRegistrationException, NotCompliantMBeanException, TTransportException, IOException, FileNodeManagerException, PathErrorException {
    	try {
			initDBDao();
		} catch (ClassNotFoundException | SQLException | DBDaoInitException e) {
			LOGGER.error("Fail to start {}!",TsFileDBConstant.GLOBAL_DB_NAME);
			return;
		}

        initFileNodeManager();

        systemDataRecovery();

        maybeInitJmx();
        registJDBCServer();
        registMonitor();
        startCloseAndMergeServer();
    }

    private void maybeInitJmx() {
        if (System.getProperty(TsFileDBConstant.REMOTE_JMX_PORT_NAME) != null) {
            LOGGER.warn("JMX settings in conf/{}.sh(Unix or OS X, if you use Windows, check conf/{}.bat) have been bypassed as the JMX connector server is "
                    + "already initialized. Please refer to {}.sh/bat for JMX configuration info", TsFileDBConstant.ENV_FILE_NAME, 
                    TsFileDBConstant.ENV_FILE_NAME, TsFileDBConstant.ENV_FILE_NAME);
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

    private void registJDBCServer() throws TTransportException, MalformedObjectNameException, InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
        jdbcMBean = new JDBCServer();
        jdbcMBean.startServer();
        ObjectName mBeanName = new ObjectName("cn.edu.thu.tsfiledb.service", "type", "JDBCServer");
        mbs.registerMBean(jdbcMBean, mBeanName);
    }
    
	private void registMonitor() throws MalformedObjectNameException, InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
		monitorMBean = new Monitor();
		ObjectName mBeanName = new ObjectName("cn.edu.thu.tsfiledb.service", "type", "Monitor");
		mbs.registerMBean(monitorMBean, mBeanName);
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
    private void systemDataRecovery() throws IOException, FileNodeManagerException, PathErrorException {
        LOGGER.info("{}: start checking write log...",TsFileDBConstant.GLOBAL_DB_NAME);
//        QueryProcessor processor = new QueryProcessor(new OverflowQPExecutor());
        WriteLogManager writeLogManager = WriteLogManager.getInstance();
        writeLogManager.recovery();
        long cnt = 0L;
        PhysicalPlan plan;
        WriteLogManager.isRecovering = true;
        while ((plan = writeLogManager.getPhysicalPlan()) != null) {
            try {
                if (plan instanceof InsertPlan) {
                    InsertPlan insertPlan = (InsertPlan) plan;
                    WriteLogRecovery.multiInsert(insertPlan);
                } else if (plan instanceof UpdatePlan) {
                    UpdatePlan updatePlan = (UpdatePlan) plan;
                    WriteLogRecovery.update(updatePlan);
                } else if (plan instanceof DeletePlan){
                    DeletePlan deletePlan = (DeletePlan) plan;
                    WriteLogRecovery.delete(deletePlan);
                }
                cnt++;
            } catch (ProcessorException e) {
                e.printStackTrace();
                throw new IOException("Error in recovery from write log");
            }
        }
        WriteLogManager.isRecovering = false;
        LOGGER.info("{}: Done. Recover operation count {}",TsFileDBConstant.GLOBAL_DB_NAME, cnt);
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
    }

    public static void main(String[] args) {
        IoTDB daemon = new IoTDB();
        daemon.active();
	    
    }

}
