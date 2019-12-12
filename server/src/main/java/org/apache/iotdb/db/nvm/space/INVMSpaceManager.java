package org.apache.iotdb.db.nvm.space;

import java.io.IOException;
import org.apache.iotdb.db.nvm.space.NVMSpaceManager.NVMSpace;

public interface INVMSpaceManager {

  NVMSpace allocate(long size) throws IOException;
}
