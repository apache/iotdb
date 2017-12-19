package cn.edu.tsinghua.iotdb.service;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.sql.SQLException;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

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

public class IoTDB implements IoTDBMBean {

	private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB.class);
	private MBeanServer mbs;
	private DBDao dBdao;
	private static JDBCServerMBean jdbcMBean;
	private MonitorMBean monitorMBean;
	private final String IOTDB_PACKAGE = "cn.edu.tsinghua.iotdb.service";
	private final String JMX_TYPE = "type";
	private final String JDBC_SERVER_STR = "JDBCServer";
	private final String MONITOR_STR = "Monitor";
	private final String IOTDB_STR = "IoTDB";

	private static class IoTDBHolder {
		private static final IoTDB INSTANCE = new IoTDB();
	}

	public static final IoTDB getInstance() {
		return IoTDBHolder.INSTANCE;
	}

	private IoTDB() {
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
		} catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException | TTransportException | IOException e) {
			LOGGER.error("{}: failed to start because: {}", TsFileDBConstant.GLOBAL_DB_NAME, e.getMessage());
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
		} catch (PathErrorException e) {
			e.printStackTrace();
		}
	}

	private void setUp() throws MalformedObjectNameException, InstanceAlreadyExistsException, MBeanRegistrationException,
			NotCompliantMBeanException, TTransportException, IOException, FileNodeManagerException, PathErrorException {
		try {
			initDBDao();
		} catch (ClassNotFoundException | SQLException | DBDaoInitException e) {
			LOGGER.error("Fail to start {}!", TsFileDBConstant.GLOBAL_DB_NAME);
			return;
		}

		initFileNodeManager();

		systemDataRecovery();

		maybeInitJmx();
		registJDBCServer();
		registMonitor();
		registIoTDBServer();
		startCloseAndMergeServer();
	}

	private void maybeInitJmx() {
		JMXServer.getInstance().start();
	}

	private void registJDBCServer() throws TTransportException, MalformedObjectNameException,
			InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
		jdbcMBean = new JDBCServer();
		jdbcMBean.startServer();
		ObjectName mBeanName = new ObjectName(IOTDB_PACKAGE, JMX_TYPE, JDBC_SERVER_STR);
		if (!mbs.isRegistered(mBeanName)) {
			mbs.registerMBean(jdbcMBean, mBeanName);
		}
	}

	private void registMonitor() throws MalformedObjectNameException, InstanceAlreadyExistsException,
			MBeanRegistrationException, NotCompliantMBeanException {
		monitorMBean = new Monitor();
		ObjectName mBeanName = new ObjectName(IOTDB_PACKAGE, JMX_TYPE, MONITOR_STR);
		if (!mbs.isRegistered(mBeanName)) {
			mbs.registerMBean(monitorMBean, mBeanName);
		}
	}

	private void registIoTDBServer() throws MalformedObjectNameException, InstanceAlreadyExistsException,
			MBeanRegistrationException, NotCompliantMBeanException {
		ObjectName mBeanName = new ObjectName(IOTDB_PACKAGE, JMX_TYPE, IOTDB_STR);
		if (!mbs.isRegistered(mBeanName)) {
			mbs.registerMBean(IoTDBHolder.INSTANCE, mBeanName);
		}
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
		LOGGER.info("{}: start checking write log...", TsFileDBConstant.GLOBAL_DB_NAME);
		// QueryProcessor processor = new QueryProcessor(new OverflowQPExecutor());
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
				} else if (plan instanceof DeletePlan) {
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
		LOGGER.info("{}: Done. Recover operation count {}", TsFileDBConstant.GLOBAL_DB_NAME, cnt);
	}

	@Override
	public void stop() throws FileNodeManagerException, IOException {
		// TODO Auto-generated method stub
		if (dBdao != null) {
			dBdao.close();
		}

		FileNodeManager.getInstance().closeAll();

		WriteLogManager.getInstance().close();

		if (jdbcMBean != null) {
			jdbcMBean.stopServer();
		}

		JMXServer.getInstance().stop();

		try {
			ObjectName montiorBeanName = new ObjectName(IOTDB_PACKAGE, JMX_TYPE, MONITOR_STR);
			if (mbs.isRegistered(montiorBeanName)) {
				mbs.unregisterMBean(montiorBeanName);
			}
		} catch (MalformedObjectNameException | MBeanRegistrationException | InstanceNotFoundException e) {
			LOGGER.error("Failed to unregisterMBean {}:{}={}", IOTDB_PACKAGE, JMX_TYPE, MONITOR_STR, e);
		}

		try {
			ObjectName jdbcBeanName = new ObjectName(IOTDB_PACKAGE, JMX_TYPE, JDBC_SERVER_STR);
			if (mbs.isRegistered(jdbcBeanName)) {
				mbs.unregisterMBean(jdbcBeanName);
			}
		} catch (MalformedObjectNameException | MBeanRegistrationException | InstanceNotFoundException e) {
			LOGGER.error("Failed to unregisterMBean {}:{}={}", IOTDB_PACKAGE, JMX_TYPE, JDBC_SERVER_STR, e);
		}

		try {
			ObjectName iotdbBeanName = new ObjectName(IOTDB_PACKAGE, JMX_TYPE, IOTDB_STR);
			if (mbs.isRegistered(iotdbBeanName)) {
				mbs.unregisterMBean(iotdbBeanName);
			}
		} catch (MalformedObjectNameException | MBeanRegistrationException | InstanceNotFoundException e) {
			LOGGER.error("Failed to unregisterMBean {}:{}={}", IOTDB_PACKAGE, JMX_TYPE, IOTDB_STR, e);
		}

		CloseMergeServer.getInstance().closeServer();
	}

	/**
	 * start the close and merge server
	 */
	private void startCloseAndMergeServer() {
		// close and merge regularly
		CloseMergeServer.getInstance().startServer();
	}

	public static void main(String[] args) {
		IoTDB daemon = new IoTDB();
		daemon.active();

	}

}
