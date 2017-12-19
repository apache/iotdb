package cn.edu.tsinghua.iotdb.qp.physical.index;


import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.index.IndexManager.IndexType;
import cn.edu.tsinghua.iotdb.index.kvmatch.KvMatchQueryRequest;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.qp.exception.QueryProcessorException;
import cn.edu.tsinghua.iotdb.qp.executor.iterator.PatternQueryDataSetIterator;
import cn.edu.tsinghua.iotdb.qp.logical.Operator;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.IndexQueryPlan;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class KvMatchIndexQueryPlan extends IndexQueryPlan {
	private Path patterPath;
	private double epsilon;
	private double alpha = 1.0d;
	private double beta = 0.0d;
	private boolean hasParameter;
	private long patterStartTime;
	private long patterEndTime;

	public KvMatchIndexQueryPlan(Path path, Path patterPath, double epsilon,
								 long patternStartTime, long patternEndTime) {
		super(path,IndexType.KvIndex);
		this.patterPath = patterPath;
		this.epsilon = epsilon;
		this.patterStartTime = patternStartTime;
		this.patterEndTime = patternEndTime;
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

	public boolean isHasParameter() {
		return hasParameter;
	}

	public void setHasParameter(boolean hasParameter) {
		this.hasParameter = hasParameter;
	}

	
	public double getEpsilon() {
		return epsilon;
	}

	public Path getPatterPath() {
		return patterPath;
	}

	public long getPatternStartTime() {
		return patterStartTime;
	}

	public long getPatternEndTime() {
		return patterEndTime;
	}

	@Override
	public Iterator<QueryDataSet> fetchQueryDateSet(int fetchSize) throws QueryProcessorException {
        MManager mManager = MManager.getInstance();
        // check path and storage group
        Path path = paths.get(0);
        if (!mManager.pathExist(path.getFullPath())) {
            throw new QueryProcessorException(String.format("The timeseries %s does not exist.", path));
        }
        try {
            mManager.getFileNameByPath(path.getFullPath());
        } catch (PathErrorException e) {
            e.printStackTrace();
            throw new QueryProcessorException(e.getMessage());
        }
        if (!mManager.pathExist(patterPath.getFullPath())) {
            throw new QueryProcessorException(String.format("The timeseries %s does not exist.", patterPath));
        }
        try {
            mManager.getFileNameByPath(patterPath.getFullPath());
        } catch (PathErrorException e) {
            e.printStackTrace();
            throw new QueryProcessorException(e.getMessage());
        }
        // check index for metadata
        if (!mManager.checkPathIndex(path.getFullPath(), IndexType.KvIndex)) {
            throw new QueryProcessorException(String.format("The timeseries %s hasn't been indexed.", path));
        }
        KvMatchQueryRequest queryRequest = KvMatchQueryRequest
                .builder(path, patterPath,
                        patterStartTime, patterEndTime,
                        epsilon)
                .startTime(startTime).endTime(endTime).build();
        queryRequest.setAlpha(alpha);
        queryRequest.setBeta(beta);
        return new PatternQueryDataSetIterator(queryRequest, fetchSize);

	}

    @Override
    public List<String> getColumnHeader() {
        List<String> columns = new ArrayList<>();
        columns.add("             Start Time");
        columns.add("               End Time");
        columns.add("               Distance");
        return columns;
    }
}
