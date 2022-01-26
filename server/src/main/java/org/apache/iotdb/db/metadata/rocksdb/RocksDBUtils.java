package org.apache.iotdb.db.metadata.rocksdb;

import org.apache.iotdb.tsfile.utils.BytesUtils;

import com.google.common.primitives.Bytes;

public class RocksDBUtils {

  protected static byte[] constructDataBlock(byte type, String data) {
    byte[] dataInBytes = data.getBytes();
    int size = dataInBytes.length;
    return Bytes.concat(new byte[] {type}, BytesUtils.intToBytes(size), dataInBytes);
  }
}
