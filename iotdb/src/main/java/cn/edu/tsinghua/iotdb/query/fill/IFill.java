package cn.edu.tsinghua.iotdb.query.fill;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.read.common.BatchData;
import cn.edu.tsinghua.tsfile.read.common.Path;

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

  public abstract BatchData getFillResult() throws ProcessorException, IOException, PathErrorException;

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