package cn.edu.thu.tsfiledb.sys.writeLog;

import java.io.IOException;

import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;

public interface WriteLogReadable {

    /**
     * check whether there is an operator in the source.
     * Note: check process starts from the end of the source.
     */
    boolean hasNextOperator() throws IOException;

    byte[] nextOperator() throws IOException;

    PhysicalPlan getPhysicalPlan() throws IOException;
}
