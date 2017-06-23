package cn.edu.thu.tsfiledb.sys.writelog;

import java.io.IOException;

import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;

public interface WriteLogReadable {

    PhysicalPlan getPhysicalPlan() throws IOException;
}
