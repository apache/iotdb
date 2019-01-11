package cn.edu.tsinghua.tsfile.read.query.dataset;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.common.RowRecord;

import java.io.IOException;
import java.util.List;


public abstract class QueryDataSet {

    protected List<Path> paths;
    protected List<TSDataType> dataTypes;

    public QueryDataSet(List<Path> paths, List<TSDataType> dataTypes) {
        this.paths = paths;
        this.dataTypes = dataTypes;
    }

    /**
     * This method is used for batch query.
     */
    public abstract boolean hasNext() throws IOException;

    /**
     * This method is used for batch query, return RowRecord.
     */
    public abstract RowRecord next() throws IOException;


    public List<Path> getPaths() {
        return paths;
    }

    public List<TSDataType> getDataTypes() {
        return dataTypes;
    }

}
