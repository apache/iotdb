package cn.edu.thu.tsfiledb.qp.logical.crud;

import java.util.HashMap;
import java.util.Map;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.logical.Operator;

public final class IndexOperator extends SFWOperator {
	
	private Path path;
	private Map<String, Integer> parameters;
	private long startTime;

	public IndexOperator(int tokenIntType) {
		super(tokenIntType);
		operatorType = Operator.OperatorType.INDEX;
		this.parameters = new HashMap<>();
	}

	public Path getPath() {
		return path;
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
}
