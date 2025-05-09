package org.apache.iotdb.session.compress;

import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.nio.ByteBuffer;
import java.util.List;

public interface ColumnDecoder {

  List<?> decode(ByteBuffer buffer, ColumnEntry columnEntry);

  Decoder getDecoder(TSDataType type, TSEncoding encodingType);
}
