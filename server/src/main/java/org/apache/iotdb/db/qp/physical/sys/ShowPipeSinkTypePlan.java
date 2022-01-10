package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.util.Collections;
import java.util.List;

public class ShowPipeSinkTypePlan extends PhysicalPlan {
    public ShowPipeSinkTypePlan() {
        super(false, Operator.OperatorType.SHOW_PIPESINKTYPE);
    }

    @Override
    public List<? extends PartialPath> getPaths() {
        return Collections.emptyList();
    }
}
