package cn.edu.tsinghua.iotdb.service;

public enum ServiceType {
	FILE_NODE_SERVICE("File Node ServerService",""),
	JMX_SERVICE("JMX ServerService","JMX ServerService"),
	JDBC_SERVICE("JDBC ServerService","JDBCService"),
	MONITOR_SERVICE("Monitor ServerService","Monitor"),
	STAT_MONITOR_SERVICE("Statistics ServerService",""),
	WAL_SERVICE("WAL ServerService",""),
	CLOSE_MERGE_SERVICE("Close&Merge ServerService",""),
	JVM_MEM_CONTROL_SERVICE("Memory Controller",""),
	AUTHORIZATION_SERVICE("Authorization ServerService", ""),
	FILE_READER_MANAGER_SERVICE("File reader manager ServerService", "");
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
