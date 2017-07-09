package cn.edu.thu.tsfiledb.service;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.exception.StartupException;

public class StartupChecks {
    private static final String ENV_FILE_NAME = "tsfile-env.sh";
    private static final Logger LOGGER = LoggerFactory.getLogger(StartupChecks.class);
    
    private final List<StartupCheck> preChecks = new ArrayList<>();
    private final List<StartupCheck> DEFALUT_TESTS = new ArrayList<>();
    
    public StartupChecks(){
	DEFALUT_TESTS.add(checkJMXPort);
    }
    
    public StartupChecks withDefaultTest(){
	preChecks.addAll(DEFALUT_TESTS);
	return this;
    }
    
    public StartupChecks withMoreTest(StartupCheck check){
	preChecks.add(check);
	return this;
    }
    
    public void verify() throws StartupException{
	for(StartupCheck check : preChecks){
	    check.execute();
	}
    }
    
    public static final StartupCheck checkJMXPort = new StartupCheck() {
        private final String REMOTE_JMX_NAME = "com.sun.management.jmxremote.port";
        private final String LOCAL_PORT_NAME = "tsfiledb.jmx.local.port";
        @Override
        public void execute() throws StartupException {
    		String jmxPort = System.getProperty(REMOTE_JMX_NAME);
    		if(jmxPort == null){
    		    LOGGER.warn("JMX is not enabled to receive remote connection. "
    		    	+ "Please check conf/{} for more info", ENV_FILE_NAME);
    		    jmxPort = System.getProperty(LOCAL_PORT_NAME);
    		    if(jmxPort == null){
    			LOGGER.error("{} missing from {}", LOCAL_PORT_NAME, ENV_FILE_NAME);
    		    }
    		}else{
    		    LOGGER.info("JMXis enabled to receive remote connection on port {}", jmxPort);
    		}
        }
    };
}
