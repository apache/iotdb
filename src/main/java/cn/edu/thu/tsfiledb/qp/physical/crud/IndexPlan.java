package cn.edu.thu.tsfiledb.qp.physical.crud;

import java.util.List;
import java.util.Map;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.logical.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;

public class IndexPlan extends PhysicalPlan {

	private Path path;
	private Map<String, Integer> parameters;
	private long startTime;

	public IndexPlan(Path path, Map<String, Integer> parameters,long startTime) {
		super(false, OperatorType.INDEX);
		this.path = path;
		this.parameters = parameters;
		this.startTime = startTime;
	}

	@Override
	public List<Path> getPaths() {
		return null;
	}

	public Path getPath() {
		return path;
	}

	public Map<String, Integer> getParameters() {
		return parameters;
	}

	public long getStartTime() {
		return startTime;
	}
}
