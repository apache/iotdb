package cn.edu.tsinghua.iotdb.sys.writelog;

import java.io.IOException;

import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;

public interface WriteLogReadable {

    PhysicalPlan getPhysicalPlan() throws IOException;

    void close() throws IOException;
}
