package cn.edu.thu.tsfiledb.service;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.conf.TsFileDBConstant;
import cn.edu.thu.tsfiledb.exception.StartupException;

public class StartupChecks {
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

        @Override
        public void execute() throws StartupException {
    		String jmxPort = System.getProperty(TsFileDBConstant.REMOTE_JMX_PORT_NAME);
    		if(jmxPort == null){
    		    LOGGER.warn("JMX is not enabled to receive remote connection. "
    		    	+ "Please check conf/{} for more info", TsFileDBConstant.ENV_FILE_NAME);
    		    jmxPort = System.getProperty(TsFileDBConstant.TSFILEDB_LOCAL_JMX_PORT_NAME);
    		    if(jmxPort == null){
    			LOGGER.warn("{} missing from {}", TsFileDBConstant.TSFILEDB_LOCAL_JMX_PORT_NAME, TsFileDBConstant.ENV_FILE_NAME);
    		    }
    		}else{
    		    LOGGER.info("JMX is enabled to receive remote connection on port {}", jmxPort);
    		}
        }
    };
}
