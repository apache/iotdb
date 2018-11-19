package cn.edu.tsinghua.iotdb.qp.physical.crud;

import cn.edu.fudan.dsm.kvmatch.iotdb.common.IndexConfig;
import cn.edu.tsinghua.iotdb.index.IndexManager.IndexType;
import cn.edu.tsinghua.iotdb.qp.logical.crud.IndexOperator.IndexOperatorType;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cn.edu.tsinghua.iotdb.qp.logical.Operator.OperatorType.INDEX;

public class IndexPlan extends
		PhysicalPlan {

	private Path path;
	private Map<String, Object> parameters;
	private final IndexOperatorType indexOperatorType;


	private final IndexType indexType;

	public IndexPlan(Path path, Map<String, Integer> parameters,long startTime,IndexOperatorType indexOperatorType, IndexType indexType) {
		super(false, INDEX);
		this.path = path;
		this.indexType = indexType;
		this.indexOperatorType = indexOperatorType;
		this.parameters = new HashMap<>();
		this.parameters.putAll(parameters);
		this.parameters.put(IndexConfig.PARAM_SINCE_TIME, startTime);
	}

	public IndexOperatorType getIndexOperatorType(){
		return indexOperatorType;
	}


	public IndexType getIndexType() {
		return indexType;
	}

	@Override
	public List<Path> getPaths() {
		List<Path> list = new ArrayList<>();
		if(path!=null){
			list.add(path);
		}
		return list;
	}

	public Map<String, Object> getParameters() {
		return parameters;
	}

}
