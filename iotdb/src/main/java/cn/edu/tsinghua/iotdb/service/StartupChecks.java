package cn.edu.tsinghua.iotdb.service;

import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.iotdb.exception.StartupException;
import cn.edu.tsinghua.iotdb.utils.CommonUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.conf.IoTDBConstant;

public class StartupChecks {
    private static final Logger LOGGER = LoggerFactory.getLogger(StartupChecks.class);
    
    private final List<StartupCheck> preChecks = new ArrayList<>();
    private final List<StartupCheck> DEFALUT_TESTS = new ArrayList<>();
    
    public StartupChecks(){
	DEFALUT_TESTS.add(checkJMXPort);
	DEFALUT_TESTS.add(checkJDK);
    }
    
    public StartupChecks withDefaultTest(){
	preChecks.addAll(DEFALUT_TESTS);
	return this;
    }
    
    public StartupChecks withMoreTest(StartupCheck check){
	preChecks.add(check);
	return this;
    }
    
    public void verify() throws StartupException {
	for(StartupCheck check : preChecks){
	    check.execute();
	}
    }
    
    public static final StartupCheck checkJMXPort = new StartupCheck() {

        @Override
        public void execute() throws StartupException {
    		String jmxPort = System.getProperty(IoTDBConstant.REMOTE_JMX_PORT_NAME);
    		if(jmxPort == null){
    		    LOGGER.warn("JMX is not enabled to receive remote connection. "
    		    	+ "Please check conf/{}.sh(Unix or OS X, if you use Windows, check conf/{}.bat) for more info", 
    		    	IoTDBConstant.ENV_FILE_NAME,IoTDBConstant.ENV_FILE_NAME);
    		    jmxPort = System.getProperty(IoTDBConstant.TSFILEDB_LOCAL_JMX_PORT_NAME);
    		    if(jmxPort == null){
    			LOGGER.warn("{} missing from {}.sh(Unix or OS X, if you use Windows, check conf/{}.bat)", 
    					IoTDBConstant.TSFILEDB_LOCAL_JMX_PORT_NAME, IoTDBConstant.ENV_FILE_NAME, IoTDBConstant.ENV_FILE_NAME);
    		    }
    		}else{
    		    LOGGER.info("JMX is enabled to receive remote connection on port {}", jmxPort);
    		}
        }
    };
    
    public static final StartupCheck checkJDK = new StartupCheck() {

        @Override
        public void execute() throws StartupException {
        	int version = CommonUtils.getJDKVersion();
        	if(version < IoTDBConstant.minSupportedJDKVerion){
        		throw new StartupException(String.format("Requires JDK version >= %d, current version is %d", IoTDBConstant.minSupportedJDKVerion, version));
        	} else{
        		LOGGER.info("JDK veriosn is {}.", version);
        	}
        }
    };
}
