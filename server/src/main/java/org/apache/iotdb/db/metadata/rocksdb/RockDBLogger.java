package org.apache.iotdb.db.metadata.rocksdb;

import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Logger;
import org.rocksdb.Options;

public class RockDBLogger extends Logger {
  private final org.slf4j.Logger logger;

  public RockDBLogger(Options options, org.slf4j.Logger logger) {
    super(options);
    this.logger = logger;
  }

  public RockDBLogger(DBOptions options, org.slf4j.Logger logger) {
    super(options);
    this.logger = logger;
  }

  @Override
  protected void log(InfoLogLevel infoLogLevel, String logMsg) {
    switch (infoLogLevel) {
      case DEBUG_LEVEL:
        logger.debug(logMsg);
        break;
      case NUM_INFO_LOG_LEVELS:
      case INFO_LEVEL:
        logger.info(logMsg);
        break;
      case WARN_LEVEL:
        logger.warn(logMsg);
        break;
      case ERROR_LEVEL:
      case FATAL_LEVEL:
      case HEADER_LEVEL:
        logger.error(logMsg);
      default:
        break;
    }
  }
}
