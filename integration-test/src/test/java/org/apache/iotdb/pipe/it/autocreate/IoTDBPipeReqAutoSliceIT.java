package org.apache.iotdb.pipe.it.autocreate;

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2AutoCreateSchema;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.utils.Pair;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2AutoCreateSchema.class})
public class IoTDBPipeReqAutoSliceIT extends AbstractPipeDualAutoIT {

  @Override
  protected void setupConfig() {
    super.setupConfig();
    senderEnv.getConfig().getCommonConfig().setPipeConnectorRequestSliceThresholdBytes(32);
    receiverEnv.getConfig().getCommonConfig().setPipeConnectorRequestSliceThresholdBytes(32);
  }

  public void pipeReqAutoSliceTest() {
    try {
      ISession senderSession = senderEnv.getSessionConnection();
      createPipe(senderSession);
      Thread.sleep(1000);
      List<Pair<Long, Integer>> data = createTestDataForInt32();
      executeDataInsertions(senderSession, data);
      verify(data);
    } catch (Exception e) {

    }
  }

  private void createPipe(ISession session)
      throws IoTDBConnectionException, StatementExecutionException {
    session.executeNonQueryStatement(
        String.format(
            "create pipe test"
                + " with source ('source'='iotdb-source','source.path'='root.test.**')"
                + " with sink ('node-urls'='%s:%s','batch.enable'='false'",
            receiverEnv.getIP(), receiverEnv.getPort()));
  }

  private void executeDataInsertions(ISession session, List<Pair<Long, Integer>> data)
      throws IoTDBConnectionException, StatementExecutionException {
    for (Pair<Long, Integer> pairs : data) {
      session.executeNonQueryStatement(
          String.format(
              "insert into root.test.db (time,status) values (%d,%d)", pairs.left, pairs.right));
    }
    session.executeNonQueryStatement("flush");
  }

  private List<Pair<Long, Integer>> createTestDataForInt32() {
    List<Pair<Long, Integer>> pairs = new ArrayList<>();
    Random random = new Random();
    for (long i = 0; i < 100; i++) {
      pairs.add(new Pair<>(i, random.nextInt()));
    }
    return pairs;
  }

  private void verify(List<Pair<Long, Integer>> data) {
    HashSet<String> set = new HashSet<>();
    for (Pair<Long, Integer> pair : data) {
      set.add(String.format("%d,%d,", pair.left, pair.right));
    }
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv, "select * form root.test.**", "time,root.test.db.status", set);
  }
}
