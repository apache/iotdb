package org.apache.iotdb.session.compress;

import org.apache.iotdb.session.RleColumnDecoder;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RpcDecoder {
  private MetaHead metaHead = new MetaHead();
  private Integer MetaHeaderLength = 0;

  public void readMetaHead(ByteBuffer buffer) {
    this.MetaHeaderLength = buffer.getInt(buffer.limit() - Integer.BYTES);
    byte[] metaBytes = new byte[MetaHeaderLength];
    buffer.position(MetaHeaderLength);
    buffer.get(metaBytes);
    this.metaHead = MetaHead.fromBytes(metaBytes);
  }

  public void decodeTimestamps(ByteBuffer buffer) {
    List<Long> timestampList = (List<Long>) decodeColumn(0, buffer);
    long[] timestamps = new long[timestampList.size()];
    for (int i = 0; i < timestampList.size(); i++) {
      timestamps[i] = timestampList.get(i);
    }
  }

  public void decodeValues(ByteBuffer buffer) {
    readMetaHead(buffer);
    int columnNum = metaHead.getColumnEntries().size();
    List<List<Object>> result = new ArrayList<>();
    for (int i = 0; i < columnNum; i++) {
      List<?> value = decodeColumn(i, buffer);
      result.add((List<Object>) value);
    }
  }

  public List<?> decodeColumn(int columnIndex, ByteBuffer buffer) {
    ColumnEntry columnEntry = metaHead.getColumnEntries().get(columnIndex);
    TSDataType dataType = columnEntry.getDataType();
    TSEncoding encodingType = columnEntry.getEncodingType();

    ColumnDecoder columnDecoder = createDecoder(dataType, encodingType);

    List<?> value = columnDecoder.decode(buffer, columnEntry);
    return value;
  }

  private ColumnDecoder createDecoder(TSDataType dataType, TSEncoding encodingType) {
    switch (encodingType) {
      case PLAIN:
        return new PlainColumnDecoder(dataType);
      case RLE:
        return new RleColumnDecoder(dataType);
      case TS_2DIFF:
        return new Ts2DiffColumnDecoder(dataType);
      default:
        throw new EncodingTypeNotSupportedException(encodingType.name());
    }
  }
}
