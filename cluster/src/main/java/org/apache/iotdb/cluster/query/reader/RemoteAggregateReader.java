package org.apache.iotdb.cluster.query.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.client.DataClient;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.reader.IAggregateReader;
import org.apache.thrift.TException;

public class RemoteAggregateReader extends RemoteSimpleSeriesReader implements IAggregateReader {

  private TSDataType dataType;

  public RemoteAggregateReader(long readerId, Node source,
      Node header,
      MetaGroupMember metaGroupMember, TSDataType dataType) {
    super(readerId, source, header, metaGroupMember);
    this.dataType = dataType;
  }

  @Override
  public PageHeader nextPageHeader() throws IOException {
    AtomicReference<ByteBuffer> resultRef = new AtomicReference<>();
    synchronized (resultRef) {
      GenericHandler<ByteBuffer> handler = new GenericHandler<>(source, resultRef);
      DataClient client = (DataClient) metaGroupMember.getDataClientPool().getClient(source);
      try {
        client.fetchPageHeader(header, readerId, handler);
        resultRef.wait(RaftServer.connectionTimeoutInMS);
      } catch (TException | InterruptedException e) {
        throw new IOException(e);
      }
    }
    ByteBuffer byteBuffer = resultRef.get();
    if (byteBuffer != null) {
      return PageHeader.deserializeFrom(byteBuffer, dataType);
    }
    return null;
  }

  @Override
  public void skipPageData() throws IOException {
    AtomicReference<Void> resultRef = new AtomicReference<>();
    DataClient client = (DataClient) metaGroupMember.getDataClientPool().getClient(source);
    synchronized (resultRef) {
      try {
        GenericHandler<Void> handler = new GenericHandler<>(source, resultRef);
        client.skipPageData(header, readerId, handler);
        resultRef.wait(RaftServer.connectionTimeoutInMS);
      } catch (TException | InterruptedException e) {
        throw new IOException(e);
      }
    }
  }
}
