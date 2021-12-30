package org.apache.iotdb.db.newsync.sender.pipe;

public class PipeConfig {
  private boolean syncDelOp;

  public void setSyncDelOp(boolean syncDelOp) {
    this.syncDelOp = syncDelOp;
  }

  public boolean getSyncDelOp() {
    return syncDelOp;
  }
}
