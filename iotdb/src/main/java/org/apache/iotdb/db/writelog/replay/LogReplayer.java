package org.apache.iotdb.db.writelog.replay;

import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

public interface LogReplayer {
    void replay(PhysicalPlan plan) throws ProcessorException;
}
