package cn.edu.tsinghua.iotdb.writelog.replay;

import cn.edu.tsinghua.iotdb.exception.ProcessorException;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;

public interface LogReplayer {
    void replay(PhysicalPlan plan) throws ProcessorException;
}
