package org.apache.iotdb.db.metadata;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

public class ColumnSchema implements Serializable {
	private static final long serialVersionUID = -8257474930341487207L;

	public String name;
	public TSDataType dataType;
	public TSEncoding encoding;
	private Map<String, String> args;

	public ColumnSchema(String name, TSDataType dataType, TSEncoding encoding) {
		this.name = name;
		this.dataType = dataType;
		this.encoding = encoding;
		this.args = new HashMap<>();
	}

	public void putKeyValueToArgs(String key, String value) {
		this.args.put(key, value);
	}

	public String getValueFromArgs(String key) {
		return args.get(key);
	}
	
	public String getName(){
		return name;
	}
	
	public TSDataType geTsDataType(){
		return dataType;
	}
	
	public TSEncoding getEncoding(){
		return encoding;
	}

	public Map<String, String> getArgsMap() {
		return args;
	}

	public void setArgsMap(Map<String, String> argsMap) {
		this.args = argsMap;
	}

}
