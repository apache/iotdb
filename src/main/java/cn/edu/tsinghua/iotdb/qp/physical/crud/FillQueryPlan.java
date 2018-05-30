package cn.edu.tsinghua.iotdb.qp.physical.crud;

import cn.edu.tsinghua.iotdb.qp.logical.Operator;
import cn.edu.tsinghua.iotdb.query.fill.IFill;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

import java.util.Map;

public class FillQueryPlan extends QueryPlan{

    private long queryTime;
    private Map<TSDataType, IFill> fillType;

    public FillQueryPlan() {
        super();
        setOperatorType(Operator.OperatorType.FILL);
    }

    public long getQueryTime() {
        return queryTime;
    }

    public void setQueryTime(long queryTime) {
        this.queryTime = queryTime;
    }

    public Map<TSDataType, IFill> getFillType() {
        return fillType;
    }

    public void setFillType(Map<TSDataType, IFill> fillType) {
        this.fillType = fillType;
    }
}
