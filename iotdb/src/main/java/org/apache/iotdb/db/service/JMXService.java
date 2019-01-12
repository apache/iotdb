package org.apache.iotdb.db.service;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.iotdb.db.exception.StartupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.StartupException;

public class JMXService implements IService {
    private static final Logger LOGGER = LoggerFactory.getLogger(JMXService.class);

    private JMXConnectorServer jmxService;

    private static class JMXServerHolder {
        private static final JMXService INSTANCE = new JMXService();
    }

    public static final JMXService getInstance() {
        return JMXServerHolder.INSTANCE;
    }

    private JMXService() {
    }

    private JMXConnectorServer createJMXServer(int port, boolean local) throws IOException {
        Map<String, Object> env = new HashMap<>();

        InetAddress serverAddress = null;
        if (local) {
            serverAddress = InetAddress.getLoopbackAddress();
            System.setProperty(IoTDBConstant.RMI_SERVER_HOST_NAME, serverAddress.getHostAddress());
        }
        int rmiPort = Integer.getInteger(IoTDBConstant.JMX_REMOTE_RMI_PORT, 0);

        JMXConnectorServer jmxServer = JMXConnectorServerFactory.newJMXConnectorServer(
                new JMXServiceURL("rmi", null, rmiPort), env, ManagementFactory.getPlatformMBeanServer());
        return jmxServer;
    }
    
    public static void registerMBean(Object mbean, String name){
		try {
			MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
			ObjectName objectName = new ObjectName(name);
			if (!mbs.isRegistered(objectName)) {
				mbs.registerMBean(mbean, objectName);
			}
		} catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
			LOGGER.error("Failed to registerMBean {}", name, e);
		}
    }
    
    public static void deregisterMBean(String name){
		try {
			MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
			ObjectName objectName = new ObjectName(name);
			if (mbs.isRegistered(objectName)) {
				mbs.unregisterMBean(objectName);
			}
		} catch (MalformedObjectNameException | MBeanRegistrationException | InstanceNotFoundException e) {
			LOGGER.error("Failed to unregisterMBean {}", name, e);
		}
    }

	@Override
	public ServiceType getID() {
		return ServiceType.JMX_SERVICE;
	}

	@Override
	public void start() throws StartupException {
		if (System.getProperty(IoTDBConstant.REMOTE_JMX_PORT_NAME) != null) {
			LOGGER.warn("JMX settings in conf/{}.sh(Unix or OS X, if you use Windows, check conf/{}.bat) have been bypassed as the JMX connector server is "
							+ "already initialized. Please refer to {}.sh/bat for JMX configuration info",
					IoTDBConstant.ENV_FILE_NAME, IoTDBConstant.ENV_FILE_NAME, IoTDBConstant.ENV_FILE_NAME);
			return;
		}
		System.setProperty(IoTDBConstant.SERVER_RMI_ID, "true");
		boolean localOnly = false;
		String jmxPort = System.getProperty(IoTDBConstant.TSFILEDB_REMOTE_JMX_PORT_NAME);

		if (jmxPort == null) {
			localOnly = true;
			jmxPort = System.getProperty(IoTDBConstant.TSFILEDB_LOCAL_JMX_PORT_NAME);
		}

		if (jmxPort == null) {
			LOGGER.warn("Failed to start {} because JMX port is undefined", this.getID().getName());
			return;
		}
		try {
			jmxService = createJMXServer(Integer.parseInt(jmxPort), localOnly);
			if (jmxService == null)
				return;
			jmxService.start();
	        LOGGER.info("{}: start {} successfully.", IoTDBConstant.GLOBAL_DB_NAME,  this.getID().getName());
		} catch (IOException e) {
			String errorMessage = String.format("Failed to start %s because of %s", this.getID().getName(), e.getMessage());
			throw new StartupException(errorMessage);
		}
	}

	@Override
	public void stop() {
		if(jmxService != null){
			try {
				jmxService.stop();
				LOGGER.info("{}: close {} successfully", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
			} catch (IOException e) {
				LOGGER.error("Failed to stop {} because of {}",this.getID().getName(), e.getMessage());
			}
		}
	}
}
