package org.apache.iotdb.session.rpccompress.encoder;

import org.apache.iotdb.session.rpccompress.ColumnEntry;
import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.PublicBAOS;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class GorillaColumnEncoder implements ColumnEncoder {
  private final Encoder encoder;
  private final TSDataType dataType;
  private ColumnEntry columnEntry;

  public GorillaColumnEncoder(TSDataType dataType) {
    this.dataType = dataType;
    this.encoder = getEncoder(dataType, TSEncoding.GORILLA);
    columnEntry = new ColumnEntry();
  }


  @Override
  public void encode(boolean[] values, ByteArrayOutputStream out) {
    // 1. Calculate the uncompressed size in bytes for the column of data.
    int unCompressedSize = getUncompressedDataSize(values.length);
    PublicBAOS outputStream = new PublicBAOS(unCompressedSize);
    try {
      // 2. Encodes the input array using the corresponding encoder from TsFile.
      for (boolean value : values) {
        encoder.encode(value, outputStream);
      }
      // 3.Flushes any buffered encoding data into the outputStream.
      encoder.flush(outputStream);
      byte[] encodedData = outputStream.toByteArray();
      // 4. Set column entry metadata
      setColumnEntry(encodedData.length, unCompressedSize);
      if (out != null) {
        out.write(encodedData);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void encode(short[] values, ByteArrayOutputStream out) {
    // 1. Calculate the uncompressed size in bytes for the column of data.
    int unCompressedSize = getUncompressedDataSize(values.length);
    PublicBAOS outputStream = new PublicBAOS(unCompressedSize);
    try {
      // 2. Encodes the input array using the corresponding encoder from TsFile.
      for (short value : values) {
        encoder.encode(value, outputStream);
      }
      // 3.Flushes any buffered encoding data into the outputStream.
      encoder.flush(outputStream);
      byte[] encodedData = outputStream.toByteArray();
      // 4. Set column entry metadata
      setColumnEntry(encodedData.length, unCompressedSize);
      if (out != null) {
        out.write(encodedData);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void encode(int[] values, ByteArrayOutputStream out) {
// 1. Calculate the uncompressed size in bytes for the column of data.
    int unCompressedSize = getUncompressedDataSize(values.length);
    PublicBAOS outputStream = new PublicBAOS(unCompressedSize);
    try {
      // 2. Encodes the input array using the corresponding encoder from TsFile.
      for (int value : values) {
        encoder.encode(value, outputStream);
      }
      // 3.Flushes any buffered encoding data into the outputStream.
      encoder.flush(outputStream);
      byte[] encodedData = outputStream.toByteArray();
      // 4. Set column entry metadata
      setColumnEntry(encodedData.length, unCompressedSize);
      if (out != null) {
        out.write(encodedData);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void encode(long[] values, ByteArrayOutputStream out) {
// 1. Calculate the uncompressed size in bytes for the column of data.
    int unCompressedSize = getUncompressedDataSize(values.length);
    PublicBAOS outputStream = new PublicBAOS(unCompressedSize);
    try {
      // 2. Encodes the input array using the corresponding encoder from TsFile.
      for (long value : values) {
        encoder.encode(value, outputStream);
      }
      // 3.Flushes any buffered encoding data into the outputStream.
      encoder.flush(outputStream);
      byte[] encodedData = outputStream.toByteArray();
      // 4. Set column entry metadata
      setColumnEntry(encodedData.length, unCompressedSize);
      if (out != null) {
        out.write(encodedData);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void encode(float[] values, ByteArrayOutputStream out) {
// 1. Calculate the uncompressed size in bytes for the column of data.
    int unCompressedSize = getUncompressedDataSize(values.length);
    PublicBAOS outputStream = new PublicBAOS(unCompressedSize);
    try {
      // 2. Encodes the input array using the corresponding encoder from TsFile.
      for (float value : values) {
        encoder.encode(value, outputStream);
      }
      // 3.Flushes any buffered encoding data into the outputStream.
      encoder.flush(outputStream);
      byte[] encodedData = outputStream.toByteArray();
      // 4. Set column entry metadata
      setColumnEntry(encodedData.length, unCompressedSize);
      if (out != null) {
        out.write(encodedData);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void encode(double[] values, ByteArrayOutputStream out) {
    // 1. Calculate the uncompressed size in bytes for the column of data.
    int unCompressedSize = getUncompressedDataSize(values.length);
    PublicBAOS outputStream = new PublicBAOS(unCompressedSize);
    try {
      // 2. Encodes the input array using the corresponding encoder from TsFile.
      for (double value : values) {
        encoder.encode(value, outputStream);
      }
      // 3.Flushes any buffered encoding data into the outputStream.
      encoder.flush(outputStream);
      byte[] encodedData = outputStream.toByteArray();
      // 4. Set column entry metadata
      setColumnEntry(encodedData.length, unCompressedSize);
      if (out != null) {
        out.write(encodedData);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void encode(Binary[] values, ByteArrayOutputStream out) {
// 1. Calculate the uncompressed size in bytes for the column of data.
    int unCompressedSize = getUncompressedDataSize(values.length, values);
    PublicBAOS outputStream = new PublicBAOS(unCompressedSize);
    try {
      // 2. Encodes the input array using the corresponding encoder from TsFile.
      for (Binary value : values) {
        encoder.encode(value, outputStream);
      }
      // 3.Flushes any buffered encoding data into the outputStream.
      encoder.flush(outputStream);
      byte[] encodedData = outputStream.toByteArray();
      // 4. Set column entry metadata
      setColumnEntry(encodedData.length, unCompressedSize);
      if (out != null) {
        out.write(encodedData);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public TSDataType getDataType() {
    return dataType;
  }

  @Override
  public TSEncoding getEncodingType() {
    return TSEncoding.GORILLA;
  }

  @Override
  public Encoder getEncoder(TSDataType type, TSEncoding encodingType) {
    return TSEncodingBuilder.getEncodingBuilder(encodingType).getEncoder(type);
  }

  @Override
  public ColumnEntry getColumnEntry() {
    return null;
  }

  private int getUncompressedDataSize(int len) {
    return getUncompressedDataSize(len, null);
  }

  private int getUncompressedDataSize(int len, Binary[] values) {
    int unCompressedSize = 0;
    switch (dataType) {
      case BOOLEAN:
        unCompressedSize = 1 * len;
        break;
      case INT32:
      case DATE:
        unCompressedSize = 4 * len;
        break;
      case INT64:
      case TIMESTAMP:
        unCompressedSize = 8 * len;
        break;
      case FLOAT:
        unCompressedSize = 4 * len;
        break;
      case DOUBLE:
        unCompressedSize = 8 * len;
        break;
      case TEXT:
      case STRING:
      case BLOB:
        for (Binary binary : values) {
          unCompressedSize += binary.getLength();
        }
        break;
      default:
        throw new UnsupportedOperationException("Doesn't support data type: " + dataType);
    }
    return unCompressedSize;
  }

  private void setColumnEntry(Integer compressedSize, Integer unCompressedSize) {
    columnEntry = new ColumnEntry(compressedSize, unCompressedSize, dataType, TSEncoding.PLAIN);
  }
}
