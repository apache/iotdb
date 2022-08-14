package org.apache.iotdb.db.qp.executor;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

public class PlanSyncer {
  static final Logger logger = LoggerFactory.getLogger(PlanSyncer.class);
  static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("QUERY_DEBUG");
  static Boolean initialized = false;
  static Boolean enableSync = false;

  static Boolean initialize() {
    if (!initialized) {
      initialized = true;
      URL url = IoTDBDescriptor.getInstance().getPropsUrl();
      if (url == null) {
        logger.warn("Couldn't load the configuration from any of the known sources.");
        return false;
      }

      try (InputStream inputStream = url.openStream()) {
        Properties properties = new Properties();
        properties.load(inputStream);
        enableSync = Boolean.parseBoolean(properties.getProperty("enable_sync"));
      } catch (FileNotFoundException e) {
        logger.warn("Fail to find config file {}", url, e);
      } catch (IOException e) {
        logger.warn("Cannot load config file, use default configuration", e);
      } catch (Exception e) {
        logger.warn("Incorrect format in config file, use default configuration", e);
      }
    }
    return initialized;
  }

  public static void SyncInsertRowPlan(InsertRowPlan plan) {
    initialize();
    if (!enableSync) {
      return;
    }
    MManager manager = MManager.getInstance();
    PartialPath devicePath = plan.getDevicePath();
    DEBUG_LOGGER.debug("Sync: {}", devicePath);
    PartialPath path1 = devicePath.concatNode("pv_time");
    DEBUG_LOGGER.debug("{} exists: {}", path1, manager.isPathExist(path1));
    PartialPath path2 = devicePath.concatNode("__synced");
    DEBUG_LOGGER.debug("{} exists: {}", path2, manager.isPathExist(path2));
  }
}
