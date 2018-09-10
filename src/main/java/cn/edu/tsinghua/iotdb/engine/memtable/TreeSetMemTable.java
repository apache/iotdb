package cn.edu.tsinghua.iotdb.engine.memtable;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

/**
 * @author Rong Kang
 */
//0910: 只在测试中使用到该类
public class TreeSetMemTable extends AbstractMemTable {
    @Override
    protected IMemSeries genMemSeries(TSDataType dataType) {
        return new TreeSetMemSeries(dataType);
    }
}
