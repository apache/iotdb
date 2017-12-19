package cn.edu.tsinghua.iotdb.service;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.conf.TsFileDBConstant;

public class JMXServer {
    private JMXConnectorServer jmxServer;
    private static final Logger LOGGER = LoggerFactory.getLogger(JMXServer.class);

    private static class JMXServerHolder {
        private static final JMXServer INSTANCE = new JMXServer();
    }

    public static final JMXServer getInstance() {
        return JMXServerHolder.INSTANCE;
    }

    private JMXServer() {

    }

    public void start() {
        if (System.getProperty(TsFileDBConstant.REMOTE_JMX_PORT_NAME) != null) {
            LOGGER.warn("JMX settings in conf/{}.sh(Unix or OS X, if you use Windows, check conf/{}.bat) have been bypassed as the JMX connector server is "
                            + "already initialized. Please refer to {}.sh/bat for JMX configuration info",
                    TsFileDBConstant.ENV_FILE_NAME, TsFileDBConstant.ENV_FILE_NAME, TsFileDBConstant.ENV_FILE_NAME);
            return;
        }
        System.setProperty(TsFileDBConstant.SERVER_RMI_ID, "true");
        boolean localOnly = false;
        String jmxPort = System.getProperty(TsFileDBConstant.TSFILEDB_REMOTE_JMX_PORT_NAME);

        if (jmxPort == null) {
            localOnly = true;
            jmxPort = System.getProperty(TsFileDBConstant.TSFILEDB_LOCAL_JMX_PORT_NAME);
        }

        if (jmxPort == null){
            LOGGER.warn("Failed to start JMX server because JMX port is undefined");
            return;
        }
        try {
            jmxServer = createJMXServer(Integer.parseInt(jmxPort), localOnly);
            if (jmxServer == null)
                return;
        } catch (IOException e) {
            LOGGER.error("Failed to start JMX server because {}", e.getMessage());
        }
    }
    
    public void stop() throws IOException{
        if(jmxServer != null){
            jmxServer.stop();
        }
        LOGGER.info("shutdown jmx server successfully");
    }

    private JMXConnectorServer createJMXServer(int port, boolean local) throws IOException {
        Map<String, Object> env = new HashMap<>();

        InetAddress serverAddress = null;
        if (local) {
            serverAddress = InetAddress.getLoopbackAddress();
            System.setProperty(TsFileDBConstant.RMI_SERVER_HOST_NAME, serverAddress.getHostAddress());
        }
        int rmiPort = Integer.getInteger(TsFileDBConstant.JMX_REMOTE_RMI_PORT, 0);

        JMXConnectorServer jmxServer = JMXConnectorServerFactory.newJMXConnectorServer(
                new JMXServiceURL("rmi", null, rmiPort), env, ManagementFactory.getPlatformMBeanServer());
        jmxServer.start();
        return jmxServer;
    }
}
