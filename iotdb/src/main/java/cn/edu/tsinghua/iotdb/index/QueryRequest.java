package cn.edu.tsinghua.iotdb.index;


import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

/**
 * The abstract class for a query request with specific parameters.
 *
 * @author Jiaye Wu
 */
public abstract class QueryRequest {

    protected Path columnPath;

    protected long startTime;

    protected long endTime;

    protected QueryRequest(Path columnPath, long startTime, long endTime) {
        this.columnPath = columnPath;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    protected QueryRequest(Path columnPath) {
        this.columnPath = columnPath;
        this.startTime = 0;
        this.endTime = Long.MAX_VALUE;
    }

    public Path getColumnPath() {
        return columnPath;
    }

    public void setColumnPath(Path columnPath) {
        this.columnPath = columnPath;
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
}
