package org.apache.iotdb.db.mpp.execution.config;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.sql.statement.metadata.SetTTLToStorageGroupStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SetTTLToStorageGroupTask implements IConfigTask{
  private static final Logger LOGGER = LoggerFactory.getLogger(SetStorageGroupTask.class);

  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final SetTTLToStorageGroupStatement setTTLToStorageGroupStatement;

  public SetTTLToStorageGroupTask(SetTTLToStorageGroupStatement setTTLToStorageGroupStatement) {
    this.setTTLToStorageGroupStatement = setTTLToStorageGroupStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute() throws InterruptedException {
    // TODO @spricoder need to implement
    return null;
  }
}
