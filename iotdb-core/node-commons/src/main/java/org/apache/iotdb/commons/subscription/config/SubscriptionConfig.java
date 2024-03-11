package org.apache.iotdb.commons.subscription.config;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscriptionConfig {

  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  /////////////////////////////// Subtask Executor ///////////////////////////////

  public int getSubscriptionSubtaskExecutorMaxThreadNum() {
    return COMMON_CONFIG.getSubscriptionSubtaskExecutorMaxThreadNum();
  }

  public int getSubscriptionMaxTabletsPerPrefetching() {
    return COMMON_CONFIG.getSubscriptionMaxTabletsPerPrefetching();
  }

  /////////////////////////////// Utils ///////////////////////////////

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionConfig.class);

  public void printAllConfigs() {
    LOGGER.info(
        "SubscriptionSubtaskExecutorMaxThreadNum: {}",
        getSubscriptionSubtaskExecutorMaxThreadNum());
    LOGGER.info(
        "SubscriptionMaxTabletsPerPrefetching: {}", getSubscriptionMaxTabletsPerPrefetching());
  }

  /////////////////////////////// Singleton ///////////////////////////////

  private SubscriptionConfig() {}

  public static SubscriptionConfig getInstance() {
    return SubscriptionConfig.SubscriptionConfigHolder.INSTANCE;
  }

  private static class SubscriptionConfigHolder {
    private static final SubscriptionConfig INSTANCE = new SubscriptionConfig();
  }
}
