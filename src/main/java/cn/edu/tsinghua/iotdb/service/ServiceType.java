package cn.edu.tsinghua.iotdb.service;

public enum ServiceType {
	FILE_NODE_SERVICE("File Node Service",""),
	JMX_SERVICE("JMX Service","JMX Service"),
	JDBC_SERVICE("JDBC Service","JDBCService"),
	MONITOR_SERVICE("Monitor Service","Monitor"),
	STAT_MONITOR_SERVICE("Statistics Service",""),
	WAL_SERVICE("WAL Service",""),
	CLOSE_MERGE_SERVICE("Close&Merge Service",""),
	JVM_MEM_CONTROL_SERVICE("Memory Controller",""),
	AUTHORIZATION_SERVICE("Authorization Service", "");
	
	private String name;  
	private String jmxName;

	private ServiceType(String name, String jmxName) {  
        this.name = name;  
        this.jmxName = jmxName;
    }  

    public String getName() {  
        return name;  
    }  

    public String getJmxName() {
    	return jmxName;
    }
}
