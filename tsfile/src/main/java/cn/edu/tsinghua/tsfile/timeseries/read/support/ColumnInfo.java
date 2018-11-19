package cn.edu.tsinghua.tsfile.timeseries.read.support;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

public class ColumnInfo {
    private String name;
    private TSDataType dataType;

    public ColumnInfo(String name, TSDataType dataType) {
        this.setName(name);
        this.setDataType(dataType);
    }

    public TSDataType getDataType() {
        return dataType;
    }

    public void setDataType(TSDataType dataType) {
        this.dataType = dataType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String toString() {
        return getName() + ":" + getDataType();
    }

    public int hashCode() {
        return getName().hashCode();
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else {
            if (o instanceof ColumnInfo) {
                return this.getName().equals(((ColumnInfo) o).getName());
            } else {
                return false;
            }
        }
    }
}
