package cn.edu.tsinghua.tsfile.file.utils;

import cn.edu.tsinghua.tsfile.common.constant.StatisticConstant;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.file.metadata.statistics.Statistics;
import cn.edu.tsinghua.tsfile.format.DataPageHeader;
import cn.edu.tsinghua.tsfile.format.DictionaryPageHeader;
import cn.edu.tsinghua.tsfile.format.Digest;
import cn.edu.tsinghua.tsfile.format.Encoding;
import cn.edu.tsinghua.tsfile.format.FileMetaData;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.format.PageType;
import cn.edu.tsinghua.tsfile.format.RowGroupBlockMetaData;

import org.apache.commons.io.IOUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * ConverterUtils is a utility class. It provide conversion between tsfile and
 * thrift metadata class. It also provides function that read/write page header
 * from/to stream
 *
 * @author XuYi xuyi556677@163.com
 */
public class ReadWriteThriftFormatUtils {
	private static final Logger LOGGER = LoggerFactory.getLogger(ReadWriteThriftFormatUtils.class);

	/**
	 * write file metadata(thrift format) to stream
	 *
	 * @param fileMetadata
	 *            file metadata to write
	 * @param to
	 *            OutputStream
	 * @throws IOException
	 *             cannot write file metadata to OutputStream
	 */
	public static void writeFileMetaData(FileMetaData fileMetadata, OutputStream to) throws IOException {
		write(fileMetadata, to);
	}

	/**
	 * read file metadata(thrift format) from stream
	 *
	 * @param from
	 *            InputStream
	 * @return metadata of TsFile
	 * @throws IOException
	 *             cannot read file metadata from OutputStream
	 */
	public static FileMetaData readFileMetaData(InputStream from) throws IOException {
		return read(from, new FileMetaData());
	}

	public static void writeRowGroupBlockMetadata(RowGroupBlockMetaData metadata, OutputStream to) throws IOException {
		write(metadata, to);
	}

	public static RowGroupBlockMetaData readRowGroupBlockMetaData(InputStream from) throws IOException {
		return read(from, new RowGroupBlockMetaData());
	}

	public static RowGroupBlockMetaData readRowGroupBlockMetaData(ITsRandomAccessFileReader reader, long offset,
			int size) throws IOException {
		reader.seek(offset);
		byte[] buf = new byte[size];
		reader.read(buf, 0, buf.length);
		ByteArrayInputStream bais = new ByteArrayInputStream(buf);

		return readRowGroupBlockMetaData(bais);
	}

	/**
	 * write DataPageHeader to output stream. For more information about
	 * DataPageHeader, see PageHeader and DataPageHeader in tsfile-format
	 * 
	 * @param uncompressedSize
	 *            uncompressed size in byte of one page size
	 * @param compressedSize
	 *            compressed size in byte of one page size
	 * @param numValues
	 *            number of value
	 * @param statistics
	 *            statistics
	 * @param numRows
	 *            number of row
	 * @param encoding
	 *            encoding type
	 * @param to
	 *            Outputstream
	 * @param max_timestamp
	 *            max timestamp
	 * @param min_timestamp
	 *            min timestamp
	 * @throws IOException
	 *             cannot write data page header to OutputStream
	 */
	public static void writeDataPageHeader(int uncompressedSize, int compressedSize, int numValues,
			Statistics<?> statistics, int numRows, TSEncoding encoding, OutputStream to, long max_timestamp,
			long min_timestamp) throws IOException {
		ReadWriteThriftFormatUtils.writePageHeader(newDataPageHeader(uncompressedSize, compressedSize, numValues,
				statistics, numRows, encoding, max_timestamp, min_timestamp), to);
	}

	private static PageHeader newDataPageHeader(int uncompressedSize, int compressedSize, int numValues,
			Statistics<?> statistics, int numRows, TSEncoding encoding, long max_timestamp, long min_timestamp) {
		PageHeader pageHeader = new PageHeader(PageType.DATA_PAGE, uncompressedSize, compressedSize);
		// TODO: pageHeader crc uncomplete

		pageHeader.setData_page_header(new DataPageHeader(numValues, numRows, Encoding.valueOf(encoding.toString()),
				max_timestamp, min_timestamp));
		if (!statistics.isEmpty()) {
			Digest digest = new Digest();
			Map<String, ByteBuffer> statisticsMap = new HashMap<>();
			// TODO add your statistics
			statisticsMap.put(StatisticConstant.MAX_VALUE, ByteBuffer.wrap(statistics.getMaxBytes()));
			statisticsMap.put(StatisticConstant.MIN_VALUE, ByteBuffer.wrap(statistics.getMinBytes()));
			statisticsMap.put(StatisticConstant.FIRST, ByteBuffer.wrap(statistics.getFirstBytes()));
			statisticsMap.put(StatisticConstant.SUM, ByteBuffer.wrap(statistics.getSumBytes()));
			statisticsMap.put(StatisticConstant.LAST, ByteBuffer.wrap(statistics.getLastBytes()));
			digest.setStatistics(statisticsMap);

			pageHeader.getData_page_header().setDigest(digest);
		}
		return pageHeader;
	}

	/**
	 * write page header(thrift format) to stream
	 *
	 * @param pageHeader
	 *            input page header
	 * @param to
	 *            OutputStream
	 * @throws IOException
	 *             cannot write page header to OutputStream
	 */
	public static void writePageHeader(PageHeader pageHeader, OutputStream to) throws IOException {
		try {
			pageHeader.write(protocol(to));
		} catch (TException e) {
			LOGGER.error("tsfile-file Utils: can not write {}", pageHeader, e);
			throw new IOException(e);
		}
	}

	/**
	 * read one page header from stream
	 *
	 * @param from
	 *            InputStream
	 * @return page header
	 * @throws IOException
	 *             cannot read page header from InputStream
	 */
	public static PageHeader readPageHeader(InputStream from) throws IOException {
		return readPageHeader(from, new PageHeader());
	}

	private static PageHeader readPageHeader(InputStream from, PageHeader header) throws IOException {
		try {
			header.read(protocol(from));
			return header;
		} catch (TException e) {
			LOGGER.error("tsfile-file Utils: can not read {}", header, e);
			throw new IOException(e);
		}
	}

	/**
	 * @param tbase
	 *            input class in thrift format
	 * @param to
	 *            OutputStream
	 * @throws IOException
	 *             exception in IO
	 */
	public static void write(TBase<?, ?> tbase, OutputStream to) throws IOException {
		try {
			TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
			to.write(serializer.serialize(tbase));
		} catch (TException e) {
			LOGGER.error("tsfile-file Utils: can not write {}", tbase, e);
			throw new IOException(e);
		}
	}

	/**
	 * @param from
	 *            InputStream
	 * @param tbase
	 *            output class in thrift format
	 * @param <T>
	 *            class in thrift-format
	 * @return Class in thrift format
	 * @throws IOException
	 *             exception in IO
	 */
	public static <T extends TBase<?, ?>> T read(InputStream from, T tbase) throws IOException {
		try {
			TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
			deserializer.deserialize(tbase, IOUtils.toByteArray(from));
			return tbase;
		} catch (TException e) {
			LOGGER.error("tsfile-file Utils: can not read {}", tbase, e);
			throw new IOException(e);
		}
	}

	private static TProtocol protocol(OutputStream to) {
		return new TCompactProtocol((new TIOStreamTransport(to)));
	}

	private static TProtocol protocol(InputStream from) {
		return new TCompactProtocol((new TIOStreamTransport(from)));
	}

	/**
	 * In current version, DictionaryPageHeader is not used
	 * 
	 * @param uncompressedSize
	 *            uncompressed size in byte of one page size
	 * @param compressedSize
	 *            compressed size in byte of one page size
	 * @param numValues
	 *            number of value
	 * @param encoding
	 *            encoding type
	 * @param to
	 *            Outputstream
	 * @throws IOException
	 *             cannot write dictionary page header to OutputStream
	 */
	@Deprecated
	public void writeDictionaryPageHeader(int uncompressedSize, int compressedSize, int numValues, TSEncoding encoding,
			OutputStream to) throws IOException {
		PageHeader pageHeader = new PageHeader(PageType.DICTIONARY_PAGE, uncompressedSize, compressedSize);
		pageHeader
				.setDictionary_page_header(new DictionaryPageHeader(numValues, Encoding.valueOf(encoding.toString())));
		ReadWriteThriftFormatUtils.writePageHeader(pageHeader, to);
	}
}
