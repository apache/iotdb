package cn.edu.tsinghua.iotdb.qp.logical.crud;


import cn.edu.tsinghua.iotdb.index.IndexManager;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

public class IndexQueryOperator extends SFWOperator {

	private final IndexManager.IndexType indexType;
	protected Path path;

	public IndexQueryOperator(int tokenIntType, IndexManager.IndexType indexType) {
		super(tokenIntType);
		this.operatorType = OperatorType.INDEXQUERY;
		this.indexType = indexType;
	}

	public Path getPath() {
		return path;
	}

	public void setPath(Path path) {
		this.path = path;
	}

	public IndexManager.IndexType getIndexType() {
		return indexType;
	}
}
