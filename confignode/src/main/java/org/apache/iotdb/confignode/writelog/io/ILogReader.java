package org.apache.iotdb.confignode.writelog.io;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * @author 辰行
 * @date 2022/10/20
 */
public interface ILogReader {

  /** release resources occupied by this object, like file streams. */
  void close();

  /**
   * return whether there exists next log to be read.
   *
   * @return whether there exists next log to be read.
   * @throws IOException
   */
  boolean hasNext() throws FileNotFoundException;

  /**
   * return the next log read from media like a WAL file and covert it to a PhysicalPlan.
   *
   * @return the next log as a PhysicalPlan
   * @throws java.util.NoSuchElementException when there are no more logs
   */
  ConfigPhysicalPlan next() throws FileNotFoundException;
}
