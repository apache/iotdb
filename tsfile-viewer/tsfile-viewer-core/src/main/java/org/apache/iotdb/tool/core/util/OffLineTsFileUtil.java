package org.apache.iotdb.tool.core.util;

import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

import java.io.IOException;

public class OffLineTsFileUtil {

  public static int fetchTsFileVersionNumber(String filePath) throws IOException {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(filePath, false)) {
      return reader.readVersionNumber();
    }
  }
}
