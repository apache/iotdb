package cn.edu.tsinghua.tsfile.timeseries.readV2.common;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

/**
 * @author Jinrui Zhang
 */
public class SeriesDescriptor {
    private Path path;
    private TSDataType dataType;

    public SeriesDescriptor(Path path, TSDataType dataType) {
        this.path = path;
        this.dataType = dataType;
    }

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    public TSDataType getDataType() {
        return dataType;
    }

    public void setDataType(TSDataType dataType) {
        this.dataType = dataType;
    }

    public String toString() {
        return this.path.toString();
    }
}
