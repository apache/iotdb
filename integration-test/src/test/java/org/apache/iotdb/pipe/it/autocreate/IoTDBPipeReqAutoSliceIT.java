package org.apache.iotdb.pipe.it.autocreate;

import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2AutoCreateSchema;

import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2AutoCreateSchema.class})
public class IoTDBPipeReqAutoSliceIT extends AbstractPipeDualAutoIT {

  @Override
  protected void setupConfig() {
    super.setupConfig();
    senderEnv.getConfig().getCommonConfig().setPipeConnectorRequestSliceThresholdBytes(2);
    receiverEnv.getConfig().getCommonConfig().setPipeConnectorRequestSliceThresholdBytes(2);
  }
}
