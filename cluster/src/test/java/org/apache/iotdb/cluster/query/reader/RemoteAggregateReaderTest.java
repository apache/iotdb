package org.apache.iotdb.cluster.query.reader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iotdb.cluster.client.ClientPool;
import org.apache.iotdb.cluster.client.DataClient;
import org.apache.iotdb.cluster.common.TestAggregateReader;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.utils.SerializeUtils;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IAggregateReader;
import org.apache.thrift.async.AsyncMethodCallback;
import org.junit.Before;
import org.junit.Test;

public class RemoteAggregateReaderTest {

  private MetaGroupMember metaGroupMember;
  private IAggregateReader remoteReader;
  private ClientPool clientPool;
  private DataClient client;

  private int pageSize = 1000;
  private int step = 10;
  private TSDataType dataType = TSDataType.INT64;

  @Before
  public void setUp() throws Exception {
    clientPool = new ClientPool(null) {
      @Override
      public AsyncClient getClient(Node node) {
        return client;
      }
    };
    client = new DataClient(null, null, TestUtils.getNode(0), clientPool){
      @Override
      public void fetchSingleSeries(Node header, long readerId,
          AsyncMethodCallback<ByteBuffer> resultHandler) {
        new Thread(() -> {
          try {
            if (!remoteReader.hasNextBatch()) {
              resultHandler.onComplete(ByteBuffer.allocate(0));
            } else {
              ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
              DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
              SerializeUtils.serializeBatchData(remoteReader.nextBatch(), dataOutputStream);
              resultHandler.onComplete(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
            }
          } catch (IOException e) {
            resultHandler.onError(e);
          }
        }).start();
      }

      @Override
      public void skipPageData(Node header, long readerId, AsyncMethodCallback<Void> resultHandler) {
        new Thread(() -> {
          try {
            remoteReader.skipPageData();
            resultHandler.onComplete(null);
          } catch (IOException e) {
            resultHandler.onError(e);
          }
        }).start();
      }

      @Override
      public void fetchPageHeader(Node header, long readerId,
          AsyncMethodCallback<ByteBuffer> resultHandler) {
        new Thread(() -> {
          try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            remoteReader.nextPageHeader().serializeTo(byteArrayOutputStream);
            resultHandler.onComplete(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
          } catch (IOException e) {
            resultHandler.onError(e);
          }
        }).start();
      }
    };

    metaGroupMember = new TestMetaGroupMember() {
      @Override
      public ClientPool getDataClientPool() {
        return clientPool;
      }
    };

    int readerDataSize = 10000;
    remoteReader = new TestAggregateReader(TestUtils.getTestBatches(0, readerDataSize, pageSize, step,
        dataType), dataType);
  }

  @Test
  public void test() throws IOException {
    RemoteAggregateReader reader = new RemoteAggregateReader(0, TestUtils.getNode(1),
        TestUtils.getNode(0), metaGroupMember, dataType);
    for (int i = 0; i < 10; i++) {
      assertTrue(reader.hasNextBatch());
      PageHeader header = reader.nextPageHeader();
      assertEquals(i * pageSize * step, header.getStartTime());
      assertEquals((i + 1) * pageSize * step - 1, header.getEndTime());
      assertEquals(pageSize, header.getNumOfValues());
      assertEquals(i * pageSize * step, header.getStatistics().getFirstValue());
      assertEquals((i + 1) * pageSize * step - 1, header.getStatistics().getLastValue());

      if (i % 2 == 0) {
        BatchData batchData = reader.nextBatch();
        for (int j = i * pageSize * step; j < (i + 1) * pageSize * step - 1; j++) {
          assertTrue(batchData.hasCurrent());
          assertEquals(j, batchData.currentTime());
          assertEquals(j, batchData.getLong());
          batchData.next();
        }
        assertFalse(batchData.hasCurrent());
      } else {
        reader.skipPageData();
      }
    }
  }
}
