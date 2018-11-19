package cn.edu.tsinghua.iotdb.query.fill;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

import java.io.IOException;

public abstract class IFill {

    long queryTime;
    TSDataType dataType;

    public IFill(TSDataType dataType, long queryTime) {
        this.dataType = dataType;
        this.queryTime = queryTime;
    }

    public IFill() {
    }

    public abstract IFill copy(Path path);

    public abstract DynamicOneColumnData getFillResult() throws ProcessorException, IOException, PathErrorException;

    public void setQueryTime(long queryTime) {
        this.queryTime = queryTime;
    }

    public void setDataType(TSDataType dataType) {
        this.dataType = dataType;
    }

    public TSDataType getDataType() {
        return this.dataType;
    }

    public long getQueryTime() {
        return this.queryTime;
    }
}
