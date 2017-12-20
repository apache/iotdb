package cn.edu.tsinghua.iotdb.qp.logical.crud;

import cn.edu.tsinghua.iotdb.index.IndexManager;
import cn.edu.tsinghua.iotdb.index.IndexManager.IndexType;
import cn.edu.tsinghua.iotdb.qp.logical.Operator;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

import java.util.HashMap;
import java.util.Map;

public final class IndexOperator extends SFWOperator {

	private Path path;
	private Map<String, Integer> parameters;
	private long startTime;
	private final IndexOperatorType  indexOperatorType;

	private final IndexType  indexType;

	public IndexOperator(int tokenIntType,IndexOperatorType indexOperatorType, IndexType indexType) {
		super(tokenIntType);
		this.indexOperatorType = indexOperatorType;
		this.indexType = indexType;
		operatorType = Operator.OperatorType.INDEX;
		this.parameters = new HashMap<>();
	}

	public Path getPath() {
		return path;
	}
	
	public IndexOperatorType  getIndexOperatorType(){
		return indexOperatorType;
	}


	public IndexType getIndexType() {
		return indexType;
	}

	public void setPath(Path path) {
		this.path = path;
	}

	public Map<String, Integer> getParameters() {
		return parameters;
	}

	public void setParameters(Map<String, Integer> parameters) {
		this.parameters = parameters;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public enum IndexOperatorType {
		CREATE_INDEX,DROP_INDEX
	}
}
