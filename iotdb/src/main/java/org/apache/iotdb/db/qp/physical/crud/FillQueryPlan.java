package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.query.fill.IFill;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.query.fill.IFill;

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
