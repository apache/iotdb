package cn.edu.tsinghua.iotdb.qp.physical.crud;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.qp.logical.Operator;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

public class IndexPlan extends PhysicalPlan {

	private Path path;
	private Map<String, Integer> parameters;
	private long startTime;

	public IndexPlan(Path path, Map<String, Integer> parameters,long startTime) {
		super(false, Operator.OperatorType.INDEX);
		this.path = path;
		this.parameters = parameters;
		this.startTime = startTime;
	}

	@Override
	public List<Path> getPaths() {
		List<Path> list = new ArrayList<>();
		if(path!=null){
			list.add(path);
		}
		return list;
	}

	public Map<String, Integer> getParameters() {
		return parameters;
	}

	public long getStartTime() {
		return startTime;
	}
}
