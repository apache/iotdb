package cn.edu.thu.tsfiledb.engine.filenode;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import cn.edu.thu.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.thu.tsfile.common.utils.ReadWriteStreamUtils;
import cn.edu.thu.tsfile.encoding.decoder.Decoder;
import cn.edu.thu.tsfile.encoding.decoder.DeltaBinaryDecoder;
import cn.edu.thu.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.format.Digest;
import cn.edu.thu.tsfile.format.PageHeader;
import cn.edu.thu.tsfile.timeseries.read.PageReader;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.metadata.MManager;

/**
 * @author CGF.
 */
public class PageTestUtils {

    private static Decoder timeDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();

    public static DynamicOneColumnData pageToDynamic(ByteArrayInputStream stream, CompressionTypeName compressionTypeName,
                                                     String deltaObjectId, String measurementId) throws IOException, PathErrorException {

        PageReader pageReader = new PageReader(stream, compressionTypeName);
        PageHeader pageHeader = pageReader.getNextPageHeader();
        Digest pageDigest = pageHeader.data_page_header.getDigest();
        TSDataType dataType = MManager.getInstance().getSeriesType(deltaObjectId + "." + measurementId);

        InputStream page = pageReader.getNextPage();
        long[] timeValues = initTimeValue(page, pageHeader.data_page_header.num_rows, false);
        Decoder valueDecoder = Decoder.getDecoderByType(pageHeader.getData_page_header().getEncoding(), dataType);

        DynamicOneColumnData ans = new DynamicOneColumnData(dataType, true);

        int i = 0;
        while (valueDecoder.hasNext(page)) {
            switch (dataType) {
                case INT32:
                    ans.putTime(timeValues[i++]);
                    ans.putInt(valueDecoder.readInt(page));
                    break;
                case INT64:
                    ans.putTime(timeValues[i++]);
                    ans.putLong(valueDecoder.readLong(page));
                    break;
                case FLOAT:
                    ans.putTime(timeValues[i++]);
                    ans.putFloat(valueDecoder.readFloat(page));
                    break;
                case DOUBLE:
                    ans.putTime(timeValues[i++]);
                    ans.putDouble(valueDecoder.readDouble(page));
                    break;
                case TEXT:
                    ans.putTime(timeValues[i++]);
                    ans.putBinary(valueDecoder.readBinary(page));
                    break;
                default:
                    throw new UnSupportedDataTypeException("UnSupported!");
            }
        }

        return ans;
    }

    private static long[] initTimeValue(InputStream page, int size, boolean skip) throws IOException {
        long[] res = null;
        int idx = 0;

        int length = ReadWriteStreamUtils.readUnsignedVarInt(page);
        byte[] buf = new byte[length];
        int readSize = page.read(buf, 0, length);

        if (!skip) {
            ByteArrayInputStream bis = new ByteArrayInputStream(buf);
            res = new long[size];
            while (timeDecoder.hasNext(bis)) {
                res[idx++] = timeDecoder.readLong(bis);
            }
        }

        return res;
    }
}