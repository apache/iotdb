package org.apache.iotdb.cluster.query.reader;

import org.apache.iotdb.cluster.client.DataClient;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.SingleSeriesQueryRequest;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.utils.SerializeUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class DatasourceInfoTest {
  private MetaGroupMember metaGroupMember;

  @Before
  public void setUp() throws IOException {
    metaGroupMember = new TestMetaGroupMember() {
      @Override
      public DataClient getDataClient(Node node) throws IOException {
        return new DataClient(null, null, TestUtils.getNode(0), null) {
          @Override
          public void querySingleSeries(SingleSeriesQueryRequest request, AsyncMethodCallback<Long> resultHandler) throws TException {
            throw new TException("Don't worry, this is the exception I constructed.");
          }
        };
      }
    };
  }

  @Test
  public void testFailedAll() throws IOException {
    PartitionGroup group = new PartitionGroup();
    group.add(TestUtils.getNode(0));
    group.add(TestUtils.getNode(1));
    group.add(TestUtils.getNode(2));

    SingleSeriesQueryRequest request = new SingleSeriesQueryRequest();
    RemoteQueryContext context = new RemoteQueryContext(1);

    DataSourceInfo sourceInfo = new DataSourceInfo(group, TSDataType.DOUBLE,
      request, context, metaGroupMember, group);
    DataClient client = sourceInfo.nextDataClient(false, Long.MIN_VALUE);

    assertEquals(client, null);
  }
}
