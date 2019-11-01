package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.read.common.Path;

import java.util.List;

/**
 * @Author: LiuDaWei
 * @Create: 2019年11月01日
 */
public class ShowVersionPlan extends PhysicalPlan {

    public ShowVersionPlan() {
        super(true);
        setOperatorType(Operator.OperatorType.VERSION);
    }

    @Override
    public List<Path> getPaths() {
        return null;
    }
}
