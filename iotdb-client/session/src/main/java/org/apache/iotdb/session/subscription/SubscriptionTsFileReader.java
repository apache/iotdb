package org.apache.iotdb.session.subscription;

import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionPolledMessage;
import org.apache.iotdb.tsfile.read.TsFileReader;

import org.apache.thrift.TException;

import java.io.IOException;

public class SubscriptionTsFileReader implements SubscriptionMessagePayload {

  private final String fileName;

  private TsFileReader reader;

  public SubscriptionTsFileReader(String fileName) {
    this.fileName = fileName;
  }

  public void open(SubscriptionPullConsumer consumer, SubscriptionCommitContext commitContext)
      throws TException, IOException, StatementExecutionException {
    SubscriptionPolledMessage rawMessage;
  }

  @Override
  public void close() throws Exception {
    reader.close();
  }
}
