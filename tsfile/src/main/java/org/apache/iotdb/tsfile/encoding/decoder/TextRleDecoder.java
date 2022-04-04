package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.encoding.encoder.TextRleEncoder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class TextRleDecoder extends Decoder {
    protected static final Logger logger = LoggerFactory.getLogger(TextRleEncoder.class);

    public TextRleDecoder() {
        super(TSEncoding.RLE);
    }

    @Override
    public boolean hasNext(ByteBuffer buffer) {
        return buffer.hasRemaining();
    }

    @Override
    public Binary readBinary(ByteBuffer buffer) {
        int size = ReadWriteForEncodingUtils.readVarInt(buffer);
        byte[] values = new byte[size * 4];
        Decoder decoder = Decoder.getDecoderByType(TSEncoding.RLE, TSDataType.INT32);
        for (int i = 0; i < size; i++) {
            int val = decoder.readInt(buffer);
            values[4 * i] = (byte) ((val >> 24) & 0xFF);
            values[4 * i + 1] = (byte) ((val >> 16) & 0xFF);
            values[4 * i + 2] = (byte) ((val >> 8) & 0xFF);
            values[4 * i + 3] = (byte) ((val) & 0xFF);
        }
        return new Binary(values);
    }

    @Override
    public void reset() {
    }
}
