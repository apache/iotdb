package cn.edu.tsinghua.iotdb.writelog.io;

import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Iterator;

public interface ILogReader extends Iterator<PhysicalPlan> {

    void open(File file) throws FileNotFoundException;

    void close();
}
