package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

public interface ISchemaFile {

  int FILE_HEADER_SIZE = 256;  // size of file header in bytes
  String FILE_FOLDER = "./pst/";  // folder to store .pmt files

  int PAGE_LENGTH = 16 * 1024;  // 16 kib for default
  int PAGE_HEADER_SIZE = 16;
  int HEADER_INDEX = 0;  // index of header page
  int INDEX_LENGTH = 4;  // 32 bit for page pointer, maximum .pmt file as 2^(32+14) bytes, 64 TiB

  short SEG_OFF_DIG = 2; // byte length of short, which is the type of index of segment inside a page
  short SEG_MAX_SIZ = 16*1024 - ISchemaFile.PAGE_HEADER_SIZE - SEG_OFF_DIG;
  short[] SEG_SIZE_LST = {1024, 2*1024, 4*1024, 8*1024, SEG_MAX_SIZ};
  int SEG_INDEX_DIGIT = 16;  // for type short
  long SEG_INDEX_MASK = 0xffffL;  // to translate address

  void write(IMNode node) throws MetadataException, IOException;
  void write(Collection<IMNode> nodes);
  void delete(IMNode node);
  void close() throws IOException;
  IMNode read(PartialPath path);
  Iterator<IMNode> getChildren(IMNode parent);

  String inspect() throws MetadataException, IOException;

  static long getGlobalIndex(int pageIndex, short segIndex) {
    return ((pageIndex << ISchemaFile.SEG_INDEX_DIGIT) | (segIndex & ISchemaFile.SEG_INDEX_MASK));
  }

  static int getPageIndex(long globalIndex) {
    return ((int) globalIndex >>> ISchemaFile.SEG_INDEX_DIGIT);
  }

  static short getSegIndex(long globalIndex) {
    return (short) (globalIndex & ISchemaFile.SEG_INDEX_MASK);
  }

  static short reEstimateSegSize(int oldSize) {
    for (short size : SEG_SIZE_LST) {
      if (oldSize < size) {
        return size;
      }
    }
    return SEG_MAX_SIZ;
  }

  /**
   * Estimate segment size for the node, by traversing each child of it and calculate its record.
   * @param node
   * @return
   */
  static short estimateSegSize(IMNode node) {
    return -1;
  }
}
