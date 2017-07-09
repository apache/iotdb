package cn.edu.thu.tsfiledb.service;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import cn.edu.thu.tsfiledb.conf.TsFileDBConstant;

public class JMXServerUtils {
    public static JMXConnectorServer createJMXServer(int port, boolean local) throws IOException {
	Map<String, Object> env = new HashMap<>();

	InetAddress serverAddress = null;
	if (local) {
	    serverAddress = InetAddress.getLoopbackAddress();
	    System.setProperty(TsFileDBConstant.RMI_SERVER_HOST_NAME, serverAddress.getHostAddress());
	}
	int rmiPort = Integer.getInteger(TsFileDBConstant.JMX_REMOTE_RMI_PORT, 0);
//	String addr = String.format("service:jmx:rmi://%s:%d/jndi/rmi://%s:%d/jmxrmi", 
//		serverAddress,
//		port,
//		serverAddress, 
//		port);
	JMXConnectorServer jmxServer = JMXConnectorServerFactory.newJMXConnectorServer(
		new JMXServiceURL("rmi", null, rmiPort), env, ManagementFactory.getPlatformMBeanServer());
	jmxServer.start();
	return jmxServer;
    }
}
