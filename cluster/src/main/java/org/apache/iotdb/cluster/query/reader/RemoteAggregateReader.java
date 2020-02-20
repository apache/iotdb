package org.apache.iotdb.cluster.query.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.client.DataClient;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IAggregateReader;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteAggregateReader extends RemoteSimpleSeriesReader implements IAggregateReader {

  private static final Logger logger = LoggerFactory.getLogger(RemoteAggregateReader.class);
  private TSDataType dataType;
  private PageHeader cachedPageHeader;

  public RemoteAggregateReader(long readerId, Node source,
      Node header,
      MetaGroupMember metaGroupMember, TSDataType dataType) {
    super(readerId, source, header, metaGroupMember);
    this.dataType = dataType;
  }

  @Override
  public boolean hasNextBatch() throws IOException {
    if (cachedPageHeader != null) {
      return true;
    }
    fetchPageHeader();
    return cachedPageHeader != null;
  }

  @Override
  public PageHeader nextPageHeader() throws IOException {
    if (!hasNextBatch()) {
      throw new NoSuchElementException();
    }
    PageHeader pageHeader = cachedPageHeader;
    cachedPageHeader = null;
    return pageHeader;
  }

  @Override
  public BatchData nextBatch() throws IOException {
    fetchBatch();
    return cachedBatch;
  }

  private void fetchPageHeader() throws IOException {
    AtomicReference<ByteBuffer> resultRef = new AtomicReference<>();
    synchronized (resultRef) {
      GenericHandler<ByteBuffer> handler = new GenericHandler<>(source, resultRef);
      DataClient client = metaGroupMember.getDataClient(source);
      try {
        client.fetchPageHeader(header, readerId, handler);
        resultRef.wait(RaftServer.connectionTimeoutInMS);
      } catch (TException | InterruptedException e) {
        throw new IOException(e);
      }
    }
    ByteBuffer byteBuffer = resultRef.get();
    if (byteBuffer != null) {
      cachedPageHeader = PageHeader.deserializeFrom(byteBuffer, dataType);
    }
  }

  @Override
  public void skipPageData() throws IOException {
    AtomicReference<Void> resultRef = new AtomicReference<>();
    DataClient client = metaGroupMember.getDataClient(source);
    synchronized (resultRef) {
      try {
        GenericHandler<Void> handler = new GenericHandler<>(source, resultRef);
        client.skipPageData(header, readerId, handler);
        resultRef.wait(RaftServer.connectionTimeoutInMS);
        cachedPageHeader = null;
      } catch (TException | InterruptedException e) {
        throw new IOException(e);
      }
    }
  }
}
