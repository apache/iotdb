package org.apache.iotdb.db.metadata.rocksdb;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;

public class InnerNameParser {

  private final byte NODE_TYPE;
  private final String pathName;

  private PartialPath partialPath;

  private static final char START_FLAG = '\u0019';
  private static final char SPLIT_FLAG = '.';

  public InnerNameParser(byte[] key) {
    NODE_TYPE = key[0];
    char[] keyConvertToCharArray = new String(key).toCharArray();

    StringBuilder stringBuilder = new StringBuilder();
    char lastChar = START_FLAG;
    for (char c : keyConvertToCharArray) {
      if (SPLIT_FLAG == lastChar || START_FLAG == lastChar) {
        lastChar = c;
        continue;
      }
      stringBuilder.append(c);
      lastChar = c;
    }
    pathName = stringBuilder.toString();
  }

  public String getPathName() {
    return pathName;
  }

  public byte getNODE_TYPE() {
    return NODE_TYPE;
  }

  public PartialPath getPartialPath() throws IllegalPathException {
    if (partialPath == null) {
      partialPath = new PartialPath(pathName);
    }
    return partialPath;
  }
}
