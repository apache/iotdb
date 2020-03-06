package org.apache.iotdb.cluster.log.logtypes;

import org.apache.iotdb.cluster.log.Log;

public class LogFactory {
  public static Log getLogByType(LogType type){
    switch (type){
      case ADD_NODE:
        return new AddNodeLog();
      case CLOSE_FILE:
        return new CloseFileLog();
      case REMOVE_NODE:
        return new RemoveNodeLog();
      case PHYSICAL_PLAN:
        return new PhysicalPlanLog();
      default:
        throw new IllegalArgumentException("unknown log type: " + type);
    }
  }
}
