package org.apache.iotdb.it.env;

import org.apache.iotdb.it.framework.IoTDBTestLogger;

import org.slf4j.Logger;

public class Cluster2Env extends AbstractEnv {
  private static final Logger logger = IoTDBTestLogger.logger;

  @Override
  public void initBeforeClass() throws InterruptedException {
    logger.debug("=======start init class=======");
    super.initEnvironment(3, 3);
  }

  @Override
  public void initBeforeTest() throws InterruptedException {
    logger.debug("=======start init test=======");
    super.initEnvironment(3, 3);
  }
}
