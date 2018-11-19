package cn.edu.tsinghua.iotdb.engine.cache;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsRowGroupBlockMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.converter.TsFileMetaDataConverter;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;

/**
 * This class is used to read metadata(<code>TsFileMetaData</code> and
 * <code>TsRowGroupBlockMetaData</code>).
 * 
 * @author liukun
 *
 */
public class TsFileMetadataUtils {

	private static final Logger LOGGER = LoggerFactory.getLogger(TsFileMetadataUtils.class);
	private static final int FOOTER_LENGTH = 4;
	private static final int MAGIC_LENGTH = TsFileIOWriter.magicStringBytes.length;

	public static TsFileMetaData getTsFileMetaData(String filePath) throws IOException {
		ITsRandomAccessFileReader randomAccessFileReader = null;
		try {
			randomAccessFileReader = new TsRandomAccessLocalFileReader(filePath);
			long l = randomAccessFileReader.length();
			randomAccessFileReader.seek(l - MAGIC_LENGTH - FOOTER_LENGTH);
			int fileMetaDataLength = randomAccessFileReader.readInt();
			randomAccessFileReader.seek(l - MAGIC_LENGTH - FOOTER_LENGTH - fileMetaDataLength);
			byte[] buf = new byte[fileMetaDataLength];
			randomAccessFileReader.read(buf, 0, buf.length);
			ByteArrayInputStream bais = new ByteArrayInputStream(buf);
			TsFileMetaData fileMetaData = new TsFileMetaDataConverter()
					.toTsFileMetadata(ReadWriteThriftFormatUtils.readFileMetaData(bais));
			return fileMetaData;
		} catch (FileNotFoundException e) {
			LOGGER.error("Can't open the tsfile {}, {}", filePath, e.getMessage());
			throw new IOException(e);
		} catch (IOException e) {
			LOGGER.error("Read the tsfile {} error, {}", filePath, e.getMessage());
			throw e;
		} finally {
			if (randomAccessFileReader != null) {
				try {
					randomAccessFileReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static TsRowGroupBlockMetaData getTsRowGroupBlockMetaData(String filePath, String deltaObjectId,
			TsFileMetaData fileMetaData) throws IOException {
		if (!fileMetaData.containsDeltaObject(deltaObjectId)) {
			return null;
		} else {
			ITsRandomAccessFileReader randomAccessFileReader = null;
			try {
				randomAccessFileReader = new TsRandomAccessLocalFileReader(filePath);
				TsRowGroupBlockMetaData blockMeta = new TsRowGroupBlockMetaData();
				long offset = fileMetaData.getDeltaObject(deltaObjectId).offset;
				int size = fileMetaData.getDeltaObject(deltaObjectId).metadataBlockSize;
				blockMeta.convertToTSF(
						ReadWriteThriftFormatUtils.readRowGroupBlockMetaData(randomAccessFileReader, offset, size));
				return blockMeta;
			} catch (FileNotFoundException e) {
				LOGGER.error("Can't open the tsfile {}, {}", filePath, e.getMessage());
				throw new IOException(e);
			} catch (IOException e) {
				LOGGER.error("Read the tsfile {} error, {}", filePath, e.getMessage());
				throw e;
			} finally {
				if (randomAccessFileReader != null) {
					try {
						randomAccessFileReader.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
}
