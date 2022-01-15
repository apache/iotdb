package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.SchemaPageOverflowException;
import org.apache.iotdb.db.exception.metadata.SegmentNotFoundException;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface ISchemaPage {

  long write(short segIdx, String key, ByteBuffer buffer)
      throws MetadataException;

  int getPageIndex();

  short getSpareSize();
  boolean isCapableForSize(short size);
  short getSegmentSize(short segId) throws SegmentNotFoundException;
  void syncPageBuffer();
  void getPageBuffer(ByteBuffer dst);
  short allocNewSegment(short size) throws IOException, SchemaPageOverflowException;
  void deleteSegment(short segId) throws SegmentNotFoundException;
  long transplantSegment(ISchemaPage srcPage, short segId, short newSegSize)
      throws SegmentNotFoundException, IOException;
  void extendsSegmentTo(ByteBuffer dstBuffer, short segId) throws SegmentNotFoundException;

  void setNextSegAddress(short segId, long address) throws SegmentNotFoundException;
  void setPrevSegAddress(short segId, long address) throws SegmentNotFoundException;
  long getNextSegAddress(short segId) throws SegmentNotFoundException;
  long getPrevSegAddress(short segId) throws SegmentNotFoundException;

  String inspect() throws SegmentNotFoundException;
}

