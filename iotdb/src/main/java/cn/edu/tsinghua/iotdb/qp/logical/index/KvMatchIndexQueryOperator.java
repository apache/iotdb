package cn.edu.tsinghua.iotdb.qp.logical.index;


import cn.edu.tsinghua.iotdb.index.IndexManager;
import cn.edu.tsinghua.iotdb.qp.logical.crud.IndexQueryOperator;
import cn.edu.tsinghua.iotdb.qp.logical.crud.SFWOperator;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

public class KvMatchIndexQueryOperator extends IndexQueryOperator {
	private Path patternPath;
	private long startTime;
	private long endTime;
	private double epsilon;
	private double alpha = 1.0;
	private double beta = 0.0;
	private boolean hasParameter;

	public KvMatchIndexQueryOperator(int tokenIntType) {
		super(tokenIntType, IndexManager.IndexType.KvIndex);
	}


	public Path getPatternPath() {
		return patternPath;
	}

	public void setPatternPath(Path patternPath) {
		this.patternPath = patternPath;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	public double getEpsilon() {
		return epsilon;
	}

	public void setEpsilon(double epsilon) {
		this.epsilon = epsilon;
	}

	public double getAlpha() {
		return alpha;
	}

	public void setAlpha(double alpha) {
		this.alpha = alpha;
	}

	public double getBeta() {
		return beta;
	}

	public void setBeta(double beta) {
		this.beta = beta;
	}

//	public boolean isHasParameter() {
//		return hasParameter;
//	}

//	public void setHasParameter(boolean hasParameter) {
//		this.hasParameter = hasParameter;
//	}
}
