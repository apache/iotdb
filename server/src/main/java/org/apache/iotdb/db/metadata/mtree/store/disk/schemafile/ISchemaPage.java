package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.SchemaPageOverflowException;
import org.apache.iotdb.db.exception.metadata.SegmentNotFoundException;
import org.apache.iotdb.db.metadata.mnode.IMNode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Queue;

public interface ISchemaPage {

  long write(short segIdx, String key, ByteBuffer buffer) throws MetadataException;

  IMNode read(short segIdx, String key) throws SegmentNotFoundException;

  void update(short segIdx, String key, ByteBuffer buffer) throws MetadataException;

  boolean hasRecordKeyInSegment(String key, short segId) throws SegmentNotFoundException;

  Queue<IMNode> getChildren(short segId) throws SegmentNotFoundException;

  void removeRecord(short segId, String key) throws SegmentNotFoundException;

  void deleteSegment(short segId) throws SegmentNotFoundException;

  int getPageIndex();

  short getSpareSize();

  short getSegmentSize(short segId) throws SegmentNotFoundException;

  void getPageBuffer(ByteBuffer dst);

  boolean isCapableForSize(short size);

  boolean isSegmentCapableFor(short segId, short size) throws SegmentNotFoundException;

  void syncPageBuffer();

  short allocNewSegment(short size) throws IOException, SchemaPageOverflowException;

  long transplantSegment(ISchemaPage srcPage, short segId, short newSegSize)
      throws MetadataException;

  void setNextSegAddress(short segId, long address) throws SegmentNotFoundException;

  void setPrevSegAddress(short segId, long address) throws SegmentNotFoundException;

  long getNextSegAddress(short segId) throws SegmentNotFoundException;

  long getPrevSegAddress(short segId) throws SegmentNotFoundException;

  String inspect() throws SegmentNotFoundException;
}
