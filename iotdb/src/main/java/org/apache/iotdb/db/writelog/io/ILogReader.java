package org.apache.iotdb.db.writelog.io;

import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Iterator;

public interface ILogReader extends Iterator<PhysicalPlan> {

    void open(File file) throws FileNotFoundException;

    void close();
}
