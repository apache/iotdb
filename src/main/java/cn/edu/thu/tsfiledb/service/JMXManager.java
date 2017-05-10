package cn.edu.thu.tsfiledb.service;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.RMISocketFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.remote.JMXAuthenticator;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXPrincipal;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.security.auth.Subject;

import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.core.joran.spi.JoranException;
import cn.edu.thu.tsfile.common.constant.SystemConstant;

public class JMXManager {
	
	static{
		try {
			String home = System.getProperty(SystemConstant.TSFILE_HOME);
			if(home != null && !home.equals("")){
				LogBackConfigLoader.load(home + File.separator + "conf" + File.separator + "logback.xml");
			}
		} catch (IOException | JoranException e) {
			System.out.println("Load configuration file error");
			e.printStackTrace();
		}
	}
	
	private static final Logger LOGGER = LoggerFactory.getLogger(JMXManager.class);

	private JMXConnectorServer connector;
	private Map<String, Object> jmxEnvironment;

	public JMXManager() {
		jmxEnvironment = new HashMap<String, Object>();
	}

	public void serice() throws IOException, TTransportException, MalformedObjectNameException,
			InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		String addr = String.format("service:jmx:rmi://%s:%d/jndi/rmi://%s:%d/jmxrmi", JDBCServerConfig.JMX_IP,
				JDBCServerConfig.JMX_PORT, JDBCServerConfig.JMX_IP, JDBCServerConfig.JMX_PORT);
		JMXServiceURL address = new JMXServiceURL(addr);

		RMISocketFactory rmiFactory = RMISocketFactory.getDefaultSocketFactory();
		LocateRegistry.createRegistry(JDBCServerConfig.JMX_PORT, null, rmiFactory);

		jmxEnvironment.put(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE, rmiFactory);

		// for auth
		JMXAuthenticator auth = createJMXAuthenticator();
		jmxEnvironment.put(JMXConnectorServer.AUTHENTICATOR, auth);

		connector = JMXConnectorServerFactory.newJMXConnectorServer(address, jmxEnvironment,
				ManagementFactory.getPlatformMBeanServer());
		JDBCServerMBean mbean = new JDBCServer();
		mbean.startServer();
		ObjectName mBeanName = new ObjectName("JDBCServerDomain", "type", "JDBCServer");
		mbs.registerMBean(mbean, mBeanName);
		connector.start();
		LOGGER.info("tsfile-service JMXManager: start JMX manager...");
	}

	public void close() throws IOException {
		connector.stop();
		LOGGER.info("tsfile-service JMXManager: close JMX manager...");
	}

	private JMXAuthenticator createJMXAuthenticator() {
		return new JMXAuthenticator() {
			public Subject authenticate(Object credentials) {
				String[] sCredentials = (String[]) credentials;
				if (null == sCredentials || sCredentials.length != 2) {
					LOGGER.error("tsfile-service JMXManager: auth info in wrong format!");
					throw new SecurityException("Authentication failed!");
				}
				String userName = sCredentials[0];
				String pValue = sCredentials[1];
				if (JDBCServerConfig.JMX_USER.equals(userName) && JDBCServerConfig.JMX_PASS.equals(pValue)) {
					Set<JMXPrincipal> principals = new HashSet<JMXPrincipal>();
					principals.add(new JMXPrincipal(userName));
					return new Subject(true, principals, Collections.EMPTY_SET, Collections.EMPTY_SET);
				}
				LOGGER.error("tsfile-service JMXManager: Authentication failed!");
				throw new SecurityException("Authentication failed!");
			}
		};
	}

	public static void main(String[] args) throws MalformedObjectNameException, InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException, TTransportException, IOException {
		JMXManager manager = new JMXManager();
		manager.serice();
	}

}
