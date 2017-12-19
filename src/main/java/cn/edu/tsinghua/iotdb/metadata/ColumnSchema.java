package cn.edu.tsinghua.iotdb.metadata;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import cn.edu.tsinghua.iotdb.index.IndexManager.IndexType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;

public class ColumnSchema implements Serializable {
	private static final long serialVersionUID = -8257474930341487207L;

	public String name;
	public TSDataType dataType;
	public TSEncoding encoding;
	private Map<String, String> args;
	private Set<IndexType> indexNameSet;

	public ColumnSchema(String name, TSDataType dataType, TSEncoding encoding) {
		this.name = name;
		this.dataType = dataType;
		this.encoding = encoding;
		this.args = new HashMap<>();
		this.indexNameSet = new HashSet<>();
	}

	public boolean isHasIndex() {
		return !indexNameSet.isEmpty();
	}

	public boolean isHasIndex(IndexType indexType) {
		return indexNameSet.contains(indexType);
	}

	public Set<IndexType> getIndexSet() {
		return indexNameSet;
	}


	public void setHasIndex(IndexType indexType) {
		this.indexNameSet.add(indexType);
	}

	public void removeIndex(IndexType indexType) { this.indexNameSet.remove(indexType); }

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
