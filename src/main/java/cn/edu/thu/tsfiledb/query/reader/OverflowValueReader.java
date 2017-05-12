package cn.edu.thu.tsfiledb.query.reader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import cn.edu.thu.tsfile.common.utils.ReadWriteStreamUtils;
import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.thu.tsfile.encoding.decoder.Decoder;
import cn.edu.thu.tsfile.file.metadata.TSDigest;
import cn.edu.thu.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.format.Digest;
import cn.edu.thu.tsfile.format.PageHeader;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.thu.tsfile.timeseries.filter.visitorImpl.DigestVisitor;
import cn.edu.thu.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.thu.tsfile.timeseries.read.PageReader;
import cn.edu.thu.tsfile.timeseries.read.ValueReader;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfiledb.query.aggregation.AggregateFunction;
import cn.edu.thu.tsfiledb.query.aggregation.AggregationResult;
import cn.edu.thu.tsfiledb.query.visitorImpl.PageAllSatisfiedVisitor;

public class OverflowValueReader extends ValueReader{

	public OverflowValueReader(long offset, long totalSize, TSDataType dataType, TSDigest digest,
			TSRandomAccessFileReader raf, List<String> enumValues, CompressionTypeName compressionTypeName,
			long rowNums) {
		super(offset, totalSize, dataType, digest, raf, enumValues, compressionTypeName, rowNums);
	}
	
	private ByteArrayInputStream initBAISForOnePage(long pageOffset) throws IOException {
		int length = (int) (this.totalSize - (pageOffset - fileOffset));
		// int length = (int) (this.totalSize + fileOffset - valueOffset);
		byte[] buf = new byte[length]; // warning
		int readSize = 0;
		raf.seek(pageOffset);
		readSize = raf.read(buf, 0, length);
		if (readSize != length) {
			throw new IOException("Expect byte size : " + length + ". Read size : " + readSize);
		}

		ByteArrayInputStream bais = new ByteArrayInputStream(buf);
		return bais;
	}
	
	@Deprecated
	public void setFreqDecoderByDatatype() {
		
	}
	
	@Deprecated
	public void initFrequenceValue(InputStream page) throws IOException {

	}
	
	@Deprecated
	public boolean frequencySatisfy(SingleSeriesFilterExpression freqFilter) {
		return true;
	}
	
	// Funciton for getValuesWithOverFlow
		public int getNextMode(int idx0, int idx1, DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse) {
			if (idx0 > updateTrue.timeLength - 2 && idx1 > updateFalse.timeLength - 2) {
				return -1;
			} else if (idx0 <= updateTrue.timeLength - 2 && idx1 > updateFalse.timeLength - 2) {
				return 0;
			} else if (idx0 > updateTrue.timeLength - 2 && idx1 <= updateFalse.timeLength - 2) {
				return 1;
			} else {
				long t0 = updateTrue.getTime(idx0);
				long t1 = updateFalse.getTime(idx1);
				return t0 < t1 ? 0 : 1;
			}
		}

		public DynamicOneColumnData getValuesWithOverFlow(DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse,
				DynamicOneColumnData insertTrue, SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
				SingleSeriesFilterExpression valueFilter, DynamicOneColumnData res, int fetchSize) throws IOException {

			// call functions without overflow if possible
			// if(updateTrue == null && updateFalse == null && insertTrue == null){
			// if(timeFilter == null && freqFilter == null && valueFilter == null){
			// return readOneColumn();
			// }else{
			// return readOneColumnUseFilter(timeFilter, freqFilter, valueFilter);
			// }
			// }

			if (res == null) {
				res = new DynamicOneColumnData(getDataType(), true);
				res.pageOffset = this.fileOffset;
				res.leftSize = this.totalSize;
				res.insertTrueIndex = 0;
			}

			// IMPORTANT!!
			if (res.pageOffset == -1) {
				res.pageOffset = this.fileOffset;
			}

			TSDigest digest = getDigest();
			DigestForFilter digestFF = new DigestForFilter(digest.min, digest.max, getDataType());
			log.info("Column Digest min and max is: " + digestFF.getMinValue() + " --- " + digestFF.getMaxValue());
			DigestVisitor digestVisitor = new DigestVisitor();
			// If not satisfied, return res with size equal to 0

			// TODO: optimize
			updateTrue = (updateTrue == null ? new DynamicOneColumnData(dataType, true) : updateTrue);
			insertTrue = (insertTrue == null ? new DynamicOneColumnData(dataType, true) : insertTrue);
			updateFalse = (updateFalse == null ? new DynamicOneColumnData(dataType, true) : updateFalse);

			if (updateTrue.length == 0 && insertTrue.length == 0 && valueFilter != null
					&& !digestVisitor.satisfy(digestFF, valueFilter)) {
				return res;
			}

			DynamicOneColumnData[] update = new DynamicOneColumnData[2];
			update[0] = updateTrue;
			update[1] = updateFalse;
			// int[] idx = new int[]{0,0};
			int[] idx = new int[] { updateTrue.curIdx, updateFalse.curIdx };
			// int idx2 = 0;
			int idx2 = insertTrue.curIdx;

			int mode = getNextMode(idx[0], idx[1], updateTrue, updateFalse);

			// initial one page from file
			ByteArrayInputStream bis = initBAISForOnePage(res.pageOffset);
			PageReader pageReader = new PageReader(bis, compressionTypeName);
			int pageCount = 0;
			// let resCount be the sum of records in last read
			// In BatchReadRecordGenerator, The ResCount needed equals to
			// (res.length - res.curIdx)
			int resCount = res.length - res.curIdx;

			// some variables for frequency calculation with overflow
			boolean hasOverflowDataInThisPage = false;

			while ((res.pageOffset - fileOffset) < totalSize && resCount < fetchSize) {
				// To help to record byte size in this process of read.
				int lastAvailable = bis.available();
				pageCount++;
				log.debug("read page {}, offset : {}", pageCount, res.pageOffset);
				PageHeader pageHeader = pageReader.getNextPageHeader();

				// construct valueFilter
				// System.out.println(res.pageOffset + "|" + fileOffset + "|" +
				// totalSize);
				Digest pageDigest = pageHeader.data_page_header.getDigest();
				DigestForFilter valueDigestFF = new DigestForFilter(pageDigest.min, pageDigest.max, getDataType());

				// construct timeFilter
				long mint = pageHeader.data_page_header.min_timestamp;
				long maxt = pageHeader.data_page_header.max_timestamp;
				DigestForFilter timeDigestFF = new DigestForFilter(mint, maxt);

				// find first interval , skip some intervals that not available
				while (mode != -1 && update[mode].getTime(idx[mode] + 1) < mint) {
					idx[mode] += 2;
					mode = getNextMode(idx[0], idx[1], updateTrue, updateFalse);
				}

				if (mode == -1 && ((valueFilter != null && !digestVisitor.satisfy(valueDigestFF, valueFilter))
						|| (timeFilter != null && !digestVisitor.satisfy(timeDigestFF, timeFilter)))) {
					pageReader.skipCurrentPage();
					res.pageOffset += lastAvailable - bis.available();
					continue;
				}
				if (mode == 0 && update[0].getTime(idx[0]) > maxt
						&& ((valueFilter != null && !digestVisitor.satisfy(valueDigestFF, valueFilter))
								|| (timeFilter != null && !digestVisitor.satisfy(timeDigestFF, timeFilter)))) {
					pageReader.skipCurrentPage();
					res.pageOffset += lastAvailable - bis.available();
					continue;
				}
				if (mode == 1 && ((update[1].getTime(idx[1]) <= mint && update[1].getTime(idx[1] + 1) >= maxt)
						|| ((valueFilter != null && !digestVisitor.satisfy(valueDigestFF, valueFilter))
								|| (timeFilter != null && !digestVisitor.satisfy(timeDigestFF, timeFilter))))) {
					pageReader.skipCurrentPage();
					res.pageOffset += lastAvailable - bis.available();
					continue;
				}

				// start traverse the hole page
				InputStream page = pageReader.getNextPage();
				// update current res's pageOffset to the start of next page.
				res.pageOffset += lastAvailable - bis.available();

				initFrequenceValue(page);
				hasOverflowDataInThisPage = checkDataChanged(mint, maxt, updateTrue, idx[0], updateFalse, idx[1],
						insertTrue, idx2, timeFilter);
//				System.out.println("Overflow: " + hasOverflowDataInThisPage);
				if (!hasOverflowDataInThisPage && !frequencySatisfy(freqFilter)) {
					continue;
				}

				long[] timeValues = initTimeValue(page, pageHeader.data_page_header.num_rows, false);

				setDecoder(Decoder.getDecoderByType(pageHeader.getData_page_header().getEncoding(), getDataType()));

				// Record the length of this res before the new records in this page
				// were put in.
				int resPreviousLength = res.length;

				SingleValueVisitor<?> timeVisitor = null;
				if(timeFilter != null){
					timeVisitor = getSingleValueVisitorByDataType(TSDataType.INT64, timeFilter);
				}
				SingleValueVisitor<?> valueVisitor = null;
				if(valueFilter != null){
					valueVisitor = getSingleValueVisitorByDataType(getDataType(), valueFilter);
				}
				
				try {
					
					int timeIdx = 0;
					switch (dataType) {
					case INT32:
						while (decoder.hasNext(page)) {
							// put insert points that less than or equals to current
							// Timestamp in page.
							while (idx2 < insertTrue.length && timeIdx < timeValues.length
									&& insertTrue.getTime(idx2) <= timeValues[timeIdx]) {
								res.putTime(insertTrue.getTime(idx2));
								res.putInt(insertTrue.getInt(idx2));
								calculateFrequency(hasOverflowDataInThisPage, freqFilter, insertTrue.getInt(idx2));
								idx2++;
								res.insertTrueIndex++;
								resCount++;
								// if equal, take value from insertTrue and skip one
								// value from page. That is to say, insert is like
								// update.
								if (insertTrue.getTime(idx2 - 1) == timeValues[timeIdx]) {
									timeIdx++;
									decoder.readInt(page);
									if (!decoder.hasNext(page)) {
										break;
									}
								}
							}
							if (!decoder.hasNext(page)) {
								break;
							}
							int v = decoder.readInt(page);
							calculateFrequency(hasOverflowDataInThisPage, freqFilter, v);
							if (mode == -1) {

								if ((valueFilter == null && timeFilter == null)
										|| (valueFilter != null && timeFilter == null
												&& valueVisitor.verify(v))
										|| (valueFilter == null && timeFilter != null
												&& timeVisitor.verify(timeValues[timeIdx]))
										|| (valueFilter != null && timeFilter != null
												&& valueVisitor.verify(v)
												&& timeVisitor.verify(timeValues[timeIdx]))) {
									res.putInt(v);
									res.putTime(timeValues[timeIdx]);
									resCount++;
								}
								timeIdx++;
							}

							if (mode == 0) {
								if (update[0].getTime(idx[0]) <= timeValues[timeIdx]
										&& timeValues[timeIdx] <= update[0].getTime(idx[0] + 1)) {
									// update the value
									if (timeFilter == null
											|| timeVisitor.verify(timeValues[timeIdx])) {
										res.putInt(update[0].getInt(idx[0] / 2));
										res.putTime(timeValues[timeIdx]);
										resCount++;
									}
								} else if ((valueFilter == null && timeFilter == null)
										|| (valueFilter != null && timeFilter == null
												&& valueVisitor.verify(v))
										|| (valueFilter == null && timeFilter != null
												&& timeVisitor.verify(timeValues[timeIdx]))
										|| (valueFilter != null && timeFilter != null
												&& valueVisitor.verify(v)
												&& timeVisitor.verify(timeValues[timeIdx]))) {
									res.putInt(v);
									res.putTime(timeValues[timeIdx]);
									resCount++;
								}
								timeIdx++;
							}

							if (mode == 1) {
								if (update[1].getTime(idx[1]) <= timeValues[timeIdx]
										&& timeValues[timeIdx] <= update[1].getTime(idx[1] + 1)) {
									// do nothing
								} else if ((valueFilter == null && timeFilter == null)
										|| (valueFilter != null && timeFilter == null
												&& valueVisitor.verify(v))
										|| (valueFilter == null && timeFilter != null
												&& timeVisitor.verify(timeValues[timeIdx]))
										|| (valueFilter != null && timeFilter != null
												&& valueVisitor.verify(v)
												&& timeVisitor.verify(timeValues[timeIdx]))) {
									res.putInt(v);
									res.putTime(timeValues[timeIdx]);
									resCount++;
								}
								timeIdx++;
							}

							// Set the interval to next position that current time
							// in page maybe be included.
							while (mode != -1 && timeIdx < timeValues.length
									&& timeValues[timeIdx] > update[mode].getTime(idx[mode] + 1)) {
								idx[mode] += 2;
								mode = getNextMode(idx[0], idx[1], update[0], update[1]);
							}
						}
						break;
					case BOOLEAN:
						while (decoder.hasNext(page)) {
							// put insert points
							while (idx2 < insertTrue.length && timeIdx < timeValues.length
									&& insertTrue.getTime(idx2) <= timeValues[timeIdx]) {
								res.putTime(insertTrue.getTime(idx2));
								res.putBoolean(insertTrue.getBoolean(idx2));
								idx2++;
								res.insertTrueIndex++;
								resCount++;
								// if equal, take value from insertTrue and skip one
								// value from page
								if (insertTrue.getTime(idx2 - 1) == timeValues[timeIdx]) {
									timeIdx++;
									decoder.readBoolean(page);
									if (!decoder.hasNext(page)) {
										break;
									}
								}
							}

							if (mode == -1) {
								boolean v = decoder.readBoolean(page);
								if ((valueFilter == null && timeFilter == null)
										|| (valueFilter != null && timeFilter == null
												&& valueVisitor.satisfyObject(v, valueFilter))
										|| (valueFilter == null && timeFilter != null
												&& timeVisitor.verify(timeValues[timeIdx]))
										|| (valueFilter != null && timeFilter != null
												&& valueVisitor.satisfyObject(v, valueFilter)
												&& timeVisitor.verify(timeValues[timeIdx]))) {
									res.putBoolean(v);
									res.putTime(timeValues[timeIdx]);
									resCount++;
								}
								timeIdx++;
							}

							if (mode == 0) {
								boolean v = decoder.readBoolean(page);
								if (update[0].getTime(idx[0]) <= timeValues[timeIdx]
										&& timeValues[timeIdx] <= update[0].getTime(idx[0] + 1)) {
									// update the value
									if (timeFilter == null
											|| timeVisitor.verify(timeValues[timeIdx])) {
										res.putBoolean(update[0].getBoolean(idx[0] / 2));
										res.putTime(timeValues[timeIdx]);
										resCount++;
									}
								} else if ((valueFilter == null && timeFilter == null)
										|| (valueFilter != null && timeFilter == null
												&& valueVisitor.satisfyObject(v, valueFilter))
										|| (valueFilter == null && timeFilter != null
												&& timeVisitor.verify(timeValues[timeIdx]))
										|| (valueFilter != null && timeFilter != null
												&& valueVisitor.satisfyObject(v, valueFilter)
												&& timeVisitor.verify(timeValues[timeIdx]))) {
									res.putBoolean(v);
									res.putTime(timeValues[timeIdx]);
									resCount++;
								}
								timeIdx++;
							}

							if (mode == 1) {
								boolean v = decoder.readBoolean(page);
								if (update[1].getTime(idx[1]) <= timeValues[timeIdx]
										&& timeValues[timeIdx] <= update[1].getTime(idx[1] + 1)) {
									// do nothing
								} else if ((valueFilter == null && timeFilter == null)
										|| (valueFilter != null && timeFilter == null
												&& valueVisitor.satisfyObject(v, valueFilter))
										|| (valueFilter == null && timeFilter != null
												&& timeVisitor.verify(timeValues[timeIdx]))
										|| (valueFilter != null && timeFilter != null
												&& valueVisitor.satisfyObject(v, valueFilter)
												&& timeVisitor.verify(timeValues[timeIdx]))) {
									res.putBoolean(v);
									res.putTime(timeValues[timeIdx]);
									resCount++;
								}
								timeIdx++;
							}

							while (mode != -1 && timeIdx < timeValues.length
									&& timeValues[timeIdx] > update[mode].getTime(idx[mode] + 1)) {
								idx[mode] += 2;
								mode = getNextMode(idx[0], idx[1], update[0], update[1]);
							}
						}
						break;
					case INT64:
						while (decoder.hasNext(page)) {
							// put insert points
							while (idx2 < insertTrue.length && timeIdx < timeValues.length
									&& insertTrue.getTime(idx2) <= timeValues[timeIdx]) {
								res.putTime(insertTrue.getTime(idx2));
								res.putLong(insertTrue.getLong(idx2));
								calculateFrequency(hasOverflowDataInThisPage, freqFilter, insertTrue.getLong(idx2));
								idx2++;
								res.insertTrueIndex++;
								resCount++;
								// if equal, take value from insertTrue and skip one
								// value from page
								if (insertTrue.getTime(idx2 - 1) == timeValues[timeIdx]) {
									timeIdx++;
									decoder.readLong(page);
									if (!decoder.hasNext(page)) {
										break;
									}
								}
							}
							if (!decoder.hasNext(page)) {
								break;
							}
							long v = decoder.readLong(page);
							calculateFrequency(hasOverflowDataInThisPage, freqFilter, v);
							if (mode == -1) {
								if ((valueFilter == null && timeFilter == null)
										|| (valueFilter != null && timeFilter == null
												&& valueVisitor.verify(v))
										|| (valueFilter == null && timeFilter != null
												&& timeVisitor.verify(timeValues[timeIdx]))
										|| (valueFilter != null && timeFilter != null
												&& valueVisitor.verify(v)
												&& timeVisitor.verify(timeValues[timeIdx]))) {
									res.putLong(v);
									res.putTime(timeValues[timeIdx]);
									resCount++;
								}
								timeIdx++;
							}

							if (mode == 0) {
								if (update[0].getTime(idx[0]) <= timeValues[timeIdx]
										&& timeValues[timeIdx] <= update[0].getTime(idx[0] + 1)) {
									// update the value,需要和高飞再商量一下这个逻辑
									if (timeFilter == null
											|| timeVisitor.verify(timeValues[timeIdx])) {
										res.putLong(update[0].getLong(idx[0] / 2));
										res.putTime(timeValues[timeIdx]);
										resCount++;
									}
								} else if ((valueFilter == null && timeFilter == null)
										|| (valueFilter != null && timeFilter == null
												&& valueVisitor.verify(v))
										|| (valueFilter == null && timeFilter != null
												&& timeVisitor.verify(timeValues[timeIdx]))
										|| (valueFilter != null && timeFilter != null
												&& valueVisitor.verify(v)
												&& timeVisitor.verify(timeValues[timeIdx]))) {
									res.putLong(v);
									res.putTime(timeValues[timeIdx]);
									resCount++;
								}
								timeIdx++;
							}

							if (mode == 1) {
								if (update[1].getTime(idx[1]) <= timeValues[timeIdx]
										&& timeValues[timeIdx] <= update[1].getTime(idx[1] + 1)) {
									// do nothing
								} else if ((valueFilter == null && timeFilter == null)
										|| (valueFilter != null && timeFilter == null
												&& valueVisitor.verify(v))
										|| (valueFilter == null && timeFilter != null
												&& timeVisitor.verify(timeValues[timeIdx]))
										|| (valueFilter != null && timeFilter != null
												&& valueVisitor.verify(v)
												&& timeVisitor.verify(timeValues[timeIdx]))) {
									res.putLong(v);
									res.putTime(timeValues[timeIdx]);
									resCount++;
								}
								timeIdx++;
							}

							while (mode != -1 && timeIdx < timeValues.length
									&& timeValues[timeIdx] > update[mode].getTime(idx[mode] + 1)) {
								idx[mode] += 2;
								mode = getNextMode(idx[0], idx[1], update[0], update[1]);
							}
						}
						break;
					case FLOAT:
						while (decoder.hasNext(page)) {
							// put insert points
							while (idx2 < insertTrue.length && timeIdx < timeValues.length
									&& insertTrue.getTime(idx2) <= timeValues[timeIdx]) {
								res.putTime(insertTrue.getTime(idx2));
								res.putFloat(insertTrue.getFloat(idx2));
								calculateFrequency(hasOverflowDataInThisPage, freqFilter, insertTrue.getFloat(idx2));
								idx2++;
								res.insertTrueIndex++;
								resCount++;
								// if equal, take value from insertTrue and skip one
								// value from page
								if (insertTrue.getTime(idx2 - 1) == timeValues[timeIdx]) {
									timeIdx++;
									decoder.readFloat(page);
									if (!decoder.hasNext(page)) {
										break;
									}
								}
							}
							if (!decoder.hasNext(page)) {
								break;
							}
							float v = decoder.readFloat(page);
							calculateFrequency(hasOverflowDataInThisPage, freqFilter, v);
							if (mode == -1) {
								if ((valueFilter == null && timeFilter == null)
										|| (valueFilter != null && timeFilter == null
												&& valueVisitor.verify(v))
										|| (valueFilter == null && timeFilter != null
												&& timeVisitor.verify(timeValues[timeIdx]))
										|| (valueFilter != null && timeFilter != null
												&& valueVisitor.verify(v)
												&& timeVisitor.verify(timeValues[timeIdx]))) {
									res.putFloat(v);
									res.putTime(timeValues[timeIdx]);
									resCount++;
								}
								timeIdx++;
							}

							if (mode == 0) {
								if (update[0].getTime(idx[0]) <= timeValues[timeIdx]
										&& timeValues[timeIdx] <= update[0].getTime(idx[0] + 1)) {
									// update the value
									if (timeFilter == null
											|| timeVisitor.verify(timeValues[timeIdx])) {
										res.putFloat(update[0].getFloat(idx[0] / 2));
										res.putTime(timeValues[timeIdx]);
										resCount++;
									}
								} else if ((valueFilter == null && timeFilter == null)
										|| (valueFilter != null && timeFilter == null
												&& valueVisitor.verify(v))
										|| (valueFilter == null && timeFilter != null
												&& timeVisitor.verify(timeValues[timeIdx]))
										|| (valueFilter != null && timeFilter != null
												&& valueVisitor.verify(v)
												&& timeVisitor.verify(timeValues[timeIdx]))) {
									res.putFloat(v);
									res.putTime(timeValues[timeIdx]);
									resCount++;
								}
								timeIdx++;
							}

							if (mode == 1) {
								if (update[1].getTime(idx[1]) <= timeValues[timeIdx]
										&& timeValues[timeIdx] <= update[1].getTime(idx[1] + 1)) {
									// do nothing
								} else if ((valueFilter == null && timeFilter == null)
										|| (valueFilter != null && timeFilter == null
												&& valueVisitor.verify(v))
										|| (valueFilter == null && timeFilter != null
												&& timeVisitor.verify(timeValues[timeIdx]))
										|| (valueFilter != null && timeFilter != null
												&& valueVisitor.verify(v)
												&& timeVisitor.verify(timeValues[timeIdx]))) {
									res.putFloat(v);
									res.putTime(timeValues[timeIdx]);
									resCount++;
								}
								timeIdx++;
							}

							while (mode != -1 && timeIdx < timeValues.length
									&& timeValues[timeIdx] > update[mode].getTime(idx[mode] + 1)) {
								idx[mode] += 2;
								mode = getNextMode(idx[0], idx[1], update[0], update[1]);
							}
						}
						break;
					case DOUBLE:
						while (decoder.hasNext(page)) {
							// put insert points
							while (idx2 < insertTrue.length && timeIdx < timeValues.length
									&& insertTrue.getTime(idx2) <= timeValues[timeIdx]) {
								res.putTime(insertTrue.getTime(idx2));
								res.putDouble(insertTrue.getDouble(idx2));
								calculateFrequency(hasOverflowDataInThisPage, freqFilter, insertTrue.getDouble(idx2));
								idx2++;
								res.insertTrueIndex++;
								resCount++;
								// if equal, take value from insertTrue and skip one
								// value from page
								if (insertTrue.getTime(idx2 - 1) == timeValues[timeIdx]) {
									timeIdx++;
									decoder.readDouble(page);
									if (!decoder.hasNext(page)) {
										break;
									}
								}
							}
							if (!decoder.hasNext(page)) {
								break;
							}
							double v = decoder.readDouble(page);
							calculateFrequency(hasOverflowDataInThisPage, freqFilter, v);
							if (mode == -1) {
								if ((valueFilter == null && timeFilter == null)
										|| (valueFilter != null && timeFilter == null
												&& valueVisitor.verify(v))
										|| (valueFilter == null && timeFilter != null
												&& timeVisitor.verify(timeValues[timeIdx]))
										|| (valueFilter != null && timeFilter != null
												&& valueVisitor.verify(v)
												&& timeVisitor.verify(timeValues[timeIdx]))) {
									res.putDouble(v);
									res.putTime(timeValues[timeIdx]);
									resCount++;
								}
								timeIdx++;
							}

							if (mode == 0) {
								if (update[0].getTime(idx[0]) <= timeValues[timeIdx]
										&& timeValues[timeIdx] <= update[0].getTime(idx[0] + 1)) {
									// update the value
									if (timeFilter == null
											|| timeVisitor.verify(timeValues[timeIdx])) {
										res.putDouble(update[0].getDouble(idx[0] / 2));
										res.putTime(timeValues[timeIdx]);
										resCount++;
									}
								} else if ((valueFilter == null && timeFilter == null)
										|| (valueFilter != null && timeFilter == null
												&& valueVisitor.verify(v))
										|| (valueFilter == null && timeFilter != null
												&& timeVisitor.verify(timeValues[timeIdx]))
										|| (valueFilter != null && timeFilter != null
												&& valueVisitor.verify(v)
												&& timeVisitor.verify(timeValues[timeIdx]))) {
									res.putDouble(v);
									res.putTime(timeValues[timeIdx]);
									resCount++;
								}
								timeIdx++;
							}

							if (mode == 1) {
								if (update[1].getTime(idx[1]) <= timeValues[timeIdx]
										&& timeValues[timeIdx] <= update[1].getTime(idx[1] + 1)) {
									// do nothing
								} else if ((valueFilter == null && timeFilter == null)
										|| (valueFilter != null && timeFilter == null
												&& valueVisitor.verify(v))
										|| (valueFilter == null && timeFilter != null
												&& timeVisitor.verify(timeValues[timeIdx]))
										|| (valueFilter != null && timeFilter != null
												&& valueVisitor.verify(v)
												&& timeVisitor.verify(timeValues[timeIdx]))) {
									res.putDouble(v);
									res.putTime(timeValues[timeIdx]);
									resCount++;
								}
								timeIdx++;
							}

							while (mode != -1 && timeIdx < timeValues.length
									&& timeValues[timeIdx] > update[mode].getTime(idx[mode] + 1)) {
								idx[mode] += 2;
								mode = getNextMode(idx[0], idx[1], update[0], update[1]);
							}
						}
						break;
					default:
						throw new IOException("Data type not support. " + dataType);
					}
				} catch (IOException e) {
					e.printStackTrace();
				}

				// Check where new records were put into res and whether the
				// frequency need to be recalculated.
				int resCurrentLength = res.length;
				if (hasOverflowDataInThisPage && freqFilter != null) {
//					boolean satisfied = frequencyCalculator.satisfy(freqFilter);
					boolean satisfied = true;
					if (!satisfied) {
						res.rollBack(resCurrentLength - resPreviousLength);
						resCount -= (resCurrentLength - resPreviousLength);
					}
				}
			}
			// Represents current Column has been read all.
			if ((res.pageOffset - fileOffset) >= totalSize) {
				res.plusRowGroupIndexAndInitPageOffset();
			}

			// Important. save curIdx for batch read
			updateTrue.curIdx = idx[0];
			updateFalse.curIdx = idx[1];
			insertTrue.curIdx = idx2;
			return res;
		}

		public AggregationResult aggreate(AggregateFunction func, DynamicOneColumnData insertTrue,
				DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse, SingleSeriesFilterExpression timeFilter,
				SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter) throws IOException {

			// initialize
			DynamicOneColumnData res = new DynamicOneColumnData(dataType, true);
			res.pageOffset = this.fileOffset;

			// Get column digest
			TSDigest digest = getDigest();
			DigestForFilter digestFF = new DigestForFilter(digest.min, digest.max, getDataType());
			log.debug("Aggretation : Column Digest min and max is: " + digestFF.getMinValue() + " --- "
					+ digestFF.getMaxValue());
			DigestVisitor digestVisitor = new DigestVisitor();

			// initialize overflow info
			updateTrue = (updateTrue == null ? new DynamicOneColumnData(dataType, true) : updateTrue);
			insertTrue = (insertTrue == null ? new DynamicOneColumnData(dataType, true) : insertTrue);
			updateFalse = (updateFalse == null ? new DynamicOneColumnData(dataType, true) : updateFalse);

			// if this column is not satisfied to the filter, then return.
			if (updateTrue.length == 0 && insertTrue.length == 0 && valueFilter != null
					&& !digestVisitor.satisfy(digestFF, valueFilter)) {
				return func.result;
			}

			DynamicOneColumnData[] update = new DynamicOneColumnData[2];
			update[0] = updateTrue;
			update[1] = updateFalse;
			int[] idx = new int[] { updateTrue.curIdx, updateFalse.curIdx };
			int idx2 = insertTrue.curIdx;

			ByteArrayInputStream bis = initBAISForOnePage(res.pageOffset);
			PageReader pageReader = new PageReader(bis, compressionTypeName);
			int pageCount = 0;

			while ((res.pageOffset - fileOffset) < totalSize) {
				int lastAvailable = bis.available();
				pageCount++;
				log.debug("read page {}, offset : {}", pageCount, res.pageOffset);
				PageHeader pageHeader = pageReader.getNextPageHeader();

				// construct value and time digest for this page
				Digest pageDigest = pageHeader.data_page_header.getDigest();
				DigestForFilter valueDigestFF = new DigestForFilter(pageDigest.min, pageDigest.max, getDataType());
				long mint = pageHeader.data_page_header.min_timestamp;
				long maxt = pageHeader.data_page_header.max_timestamp;
				DigestForFilter timeDigestFF = new DigestForFilter(mint, maxt);
				
				
				int mode = getNextMode(idx[0], idx[1], updateTrue, updateFalse);
				// find first interval , skip some intervals that not available
				while (mode != -1 && update[mode].getTime(idx[mode] + 1) < mint) {
					idx[mode] += 2;
					mode = getNextMode(idx[0], idx[1], updateTrue, updateFalse);
				}

				//check whether current page is satisfied to filters.
				if (mode == -1 && ((valueFilter != null && !digestVisitor.satisfy(valueDigestFF, valueFilter))
						|| (timeFilter != null && !digestVisitor.satisfy(timeDigestFF, timeFilter)))) {
					pageReader.skipCurrentPage();
					res.pageOffset += lastAvailable - bis.available();
					continue;
				}
				if (mode == 0 && update[0].getTime(idx[0]) > maxt
						&& ((valueFilter != null && !digestVisitor.satisfy(valueDigestFF, valueFilter))
								|| (timeFilter != null && !digestVisitor.satisfy(timeDigestFF, timeFilter)))) {
					pageReader.skipCurrentPage();
					res.pageOffset += lastAvailable - bis.available();
					continue;
				}
				if (mode == 1 && ((update[1].getTime(idx[1]) <= mint && update[1].getTime(idx[1] + 1) >= maxt)
						|| ((valueFilter != null && !digestVisitor.satisfy(valueDigestFF, valueFilter))
								|| (timeFilter != null && !digestVisitor.satisfy(timeDigestFF, timeFilter))))) {
					pageReader.skipCurrentPage();
					res.pageOffset += lastAvailable - bis.available();
					continue;
				}
				
				//Get the InputStream for this page
				InputStream page = pageReader.getNextPage();
				// update current res's pageOffset to the start of next page.
				res.pageOffset += lastAvailable - bis.available();
				initFrequenceValue(page);
				boolean hasOverflowDataInThisPage = checkDataChangedForAggregation(mint, maxt, valueDigestFF 
						, updateTrue, idx[0], updateFalse, idx[1],insertTrue, idx2
						, timeFilter, freqFilter, valueFilter);
				log.debug("Having Overflow info in this page : {}", hasOverflowDataInThisPage);
				
				//If there is no overflow data in this page
				boolean needToReadData = true;
				if(!hasOverflowDataInThisPage){
					needToReadData = !func.calculateFromPageHeader(pageHeader);
				}
				
				if(needToReadData){
					//Get all time values in this page
					long[] timeValues = initTimeValue(page, pageHeader.data_page_header.num_rows, false);
					//Set Decoder for current page
					setDecoder(Decoder.getDecoderByType(pageHeader.getData_page_header().getEncoding(), getDataType()));
					
					//clear data in res to make the res only store the data in current page;
					res = readOnePageWithOverflow(hasOverflowDataInThisPage, idx, timeValues, page, 
							pageHeader, res, timeFilter, freqFilter, valueFilter, insertTrue, update);
					func.calculateFromDataInThisPage(res);
				}
			}
			
			//Record the current index for overflow info
			insertTrue.curIdx = idx2;
			updateTrue.curIdx = idx[0];
			updateFalse.curIdx = idx[1];
			
			return func.result;
		}

		private DynamicOneColumnData readOnePageWithOverflow(boolean hasOverflowDataInThisPage, int[] idx,
				long[] timeValues, InputStream page, PageHeader pageHeader, DynamicOneColumnData res,
				SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter,
				DynamicOneColumnData insertTrue, DynamicOneColumnData[] update) throws IOException {

			// Calculate current mode
			int mode = getNextMode(idx[0], idx[1], update[0], update[1]);

			try {
				SingleValueVisitor<?> timeVisitor = null;
				if(timeFilter != null){
					timeVisitor = getSingleValueVisitorByDataType(TSDataType.INT64, timeFilter);
				}
				SingleValueVisitor<?> valueVisitor = null;
				if(valueFilter != null){
					valueVisitor = getSingleValueVisitorByDataType(getDataType(), valueFilter);
				}
				
				int timeIdx = 0;
				switch (dataType) {
				case INT32:
					while (decoder.hasNext(page)) {
						// put insert points that less than or equals to current
						// Timestamp in page.
						while (insertTrue.curIdx < insertTrue.length && timeIdx < timeValues.length
								&& insertTrue.getTime(insertTrue.curIdx) <= timeValues[timeIdx]) {
							res.putTime(insertTrue.getTime(insertTrue.curIdx));
							res.putInt(insertTrue.getInt(insertTrue.curIdx));
							insertTrue.curIdx++;
							res.insertTrueIndex++;
							calculateFrequency(hasOverflowDataInThisPage, freqFilter, insertTrue.getInt(insertTrue.curIdx));
							// if equal, take value from insertTrue and skip one
							// value from page. That is to say, insert is like
							// update.
							if (insertTrue.getTime(insertTrue.curIdx - 1) == timeValues[timeIdx]) {
								timeIdx++;
								decoder.readInt(page);
								if (!decoder.hasNext(page)) {
									break;
								}
							}
						}
						if (!decoder.hasNext(page)) {
							break;
						}
						int v = decoder.readInt(page);
						calculateFrequency(hasOverflowDataInThisPage, freqFilter, v);
						if (mode == -1) {

							if ((valueFilter == null && timeFilter == null)
									|| (valueFilter != null && timeFilter == null
											&& valueVisitor.verify(v))
									|| (valueFilter == null && timeFilter != null
											&& timeVisitor.verify(timeValues[timeIdx]))
									|| (valueFilter != null && timeFilter != null
											&& valueVisitor.verify(v)
											&& timeVisitor.verify(timeValues[timeIdx]))) {
								res.putInt(v);
								res.putTime(timeValues[timeIdx]);
							}
							timeIdx++;
						}

						if (mode == 0) {
							if (update[0].getTime(idx[0]) <= timeValues[timeIdx]
									&& timeValues[timeIdx] <= update[0].getTime(idx[0] + 1)) {
								// update the value
								if (timeFilter == null
										|| timeVisitor.verify(timeValues[timeIdx])) {
									res.putInt(update[0].getInt(idx[0] / 2));
									res.putTime(timeValues[timeIdx]);
								}
							} else if ((valueFilter == null && timeFilter == null)
									|| (valueFilter != null && timeFilter == null
											&& valueVisitor.verify(v))
									|| (valueFilter == null && timeFilter != null
											&& timeVisitor.verify(timeValues[timeIdx]))
									|| (valueFilter != null && timeFilter != null
											&& valueVisitor.verify(v)
											&& timeVisitor.verify(timeValues[timeIdx]))) {
								res.putInt(v);
								res.putTime(timeValues[timeIdx]);
							}
							timeIdx++;
						}

						if (mode == 1) {
							if (update[1].getTime(idx[1]) <= timeValues[timeIdx]
									&& timeValues[timeIdx] <= update[1].getTime(idx[1] + 1)) {
								// do nothing
							} else if ((valueFilter == null && timeFilter == null)
									|| (valueFilter != null && timeFilter == null
											&& valueVisitor.verify(v))
									|| (valueFilter == null && timeFilter != null
											&& timeVisitor.verify(timeValues[timeIdx]))
									|| (valueFilter != null && timeFilter != null
											&& valueVisitor.verify(v)
											&& timeVisitor.verify(timeValues[timeIdx]))) {
								res.putInt(v);
								res.putTime(timeValues[timeIdx]);
							}
							timeIdx++;
						}

						// Set the interval to next position that current time
						// in page maybe be included.
						while (mode != -1 && timeIdx < timeValues.length
								&& timeValues[timeIdx] > update[mode].getTime(idx[mode] + 1)) {
							idx[mode] += 2;
							mode = getNextMode(idx[0], idx[1], update[0], update[1]);
						}
					}
					break;
				case BOOLEAN:
					while (decoder.hasNext(page)) {
						// put insert points
						while (insertTrue.curIdx < insertTrue.length && timeIdx < timeValues.length
								&& insertTrue.getTime(insertTrue.curIdx) <= timeValues[timeIdx]) {
							res.putTime(insertTrue.getTime(insertTrue.curIdx));
							res.putBoolean(insertTrue.getBoolean(insertTrue.curIdx));
							insertTrue.curIdx++;
							res.insertTrueIndex++;
							// if equal, take value from insertTrue and skip one
							// value from page
							if (insertTrue.getTime(insertTrue.curIdx - 1) == timeValues[timeIdx]) {
								timeIdx++;
								decoder.readBoolean(page);
								if (!decoder.hasNext(page)) {
									break;
								}
							}
						}

						if (mode == -1) {
							boolean v = decoder.readBoolean(page);
							if ((valueFilter == null && timeFilter == null)
									|| (valueFilter != null && timeFilter == null
											&& valueVisitor.satisfyObject(v, valueFilter))
									|| (valueFilter == null && timeFilter != null
											&& timeVisitor.verify(timeValues[timeIdx]))
									|| (valueFilter != null && timeFilter != null
											&& valueVisitor.satisfyObject(v, valueFilter)
											&& timeVisitor.verify(timeValues[timeIdx]))) {
								res.putBoolean(v);
								res.putTime(timeValues[timeIdx]);
							}
							timeIdx++;
						}

						if (mode == 0) {
							boolean v = decoder.readBoolean(page);
							if (update[0].getTime(idx[0]) <= timeValues[timeIdx]
									&& timeValues[timeIdx] <= update[0].getTime(idx[0] + 1)) {
								// update the value
								if (timeFilter == null
										|| timeVisitor.verify(timeValues[timeIdx])) {
									res.putBoolean(update[0].getBoolean(idx[0] / 2));
									res.putTime(timeValues[timeIdx]);
								}
							} else if ((valueFilter == null && timeFilter == null)
									|| (valueFilter != null && timeFilter == null
											&& valueVisitor.satisfyObject(v, valueFilter))
									|| (valueFilter == null && timeFilter != null
											&& timeVisitor.verify(timeValues[timeIdx]))
									|| (valueFilter != null && timeFilter != null
											&& valueVisitor.satisfyObject(v, valueFilter)
											&& timeVisitor.verify(timeValues[timeIdx]))) {
								res.putBoolean(v);
								res.putTime(timeValues[timeIdx]);
							}
							timeIdx++;
						}

						if (mode == 1) {
							boolean v = decoder.readBoolean(page);
							if (update[1].getTime(idx[1]) <= timeValues[timeIdx]
									&& timeValues[timeIdx] <= update[1].getTime(idx[1] + 1)) {
								// do nothing
							} else if ((valueFilter == null && timeFilter == null)
									|| (valueFilter != null && timeFilter == null
											&& valueVisitor.satisfyObject(v, valueFilter))
									|| (valueFilter == null && timeFilter != null
											&& timeVisitor.verify(timeValues[timeIdx]))
									|| (valueFilter != null && timeFilter != null
											&& valueVisitor.satisfyObject(v, valueFilter)
											&& timeVisitor.verify(timeValues[timeIdx]))) {
								res.putBoolean(v);
								res.putTime(timeValues[timeIdx]);
							}
							timeIdx++;
						}

						while (mode != -1 && timeIdx < timeValues.length
								&& timeValues[timeIdx] > update[mode].getTime(idx[mode] + 1)) {
							idx[mode] += 2;
							mode = getNextMode(idx[0], idx[1], update[0], update[1]);
						}
					}
					break;
				case INT64:
					while (decoder.hasNext(page)) {
						// put insert points
						while (insertTrue.curIdx < insertTrue.length && timeIdx < timeValues.length
								&& insertTrue.getTime(insertTrue.curIdx) <= timeValues[timeIdx]) {
							res.putTime(insertTrue.getTime(insertTrue.curIdx));
							res.putLong(insertTrue.getLong(insertTrue.curIdx));
							insertTrue.curIdx++;
							res.insertTrueIndex++;
							calculateFrequency(hasOverflowDataInThisPage, freqFilter,
									insertTrue.getLong(insertTrue.curIdx));
							// if equal, take value from insertTrue and skip one
							// value from page
							if (insertTrue.getTime(insertTrue.curIdx - 1) == timeValues[timeIdx]) {
								timeIdx++;
								decoder.readLong(page);
								if (!decoder.hasNext(page)) {
									break;
								}
							}
						}
						if (!decoder.hasNext(page)) {
							break;
						}
						long v = decoder.readLong(page);
						calculateFrequency(hasOverflowDataInThisPage, freqFilter, v);
						if (mode == -1) {
							if ((valueFilter == null && timeFilter == null)
									|| (valueFilter != null && timeFilter == null
											&& valueVisitor.verify(v))
									|| (valueFilter == null && timeFilter != null
											&& timeVisitor.verify(timeValues[timeIdx]))
									|| (valueFilter != null && timeFilter != null
											&& valueVisitor.verify(v)
											&& timeVisitor.verify(timeValues[timeIdx]))) {
								res.putLong(v);
								res.putTime(timeValues[timeIdx]);
							}
							timeIdx++;
						}

						if (mode == 0) {
							if (update[0].getTime(idx[0]) <= timeValues[timeIdx]
									&& timeValues[timeIdx] <= update[0].getTime(idx[0] + 1)) {
								// update the value,需要和高飞再商量一下这个逻辑
								if (timeFilter == null
										|| timeVisitor.verify(timeValues[timeIdx])) {
									res.putLong(update[0].getLong(idx[0] / 2));
									res.putTime(timeValues[timeIdx]);
								}
							} else if ((valueFilter == null && timeFilter == null)
									|| (valueFilter != null && timeFilter == null
											&& valueVisitor.verify(v))
									|| (valueFilter == null && timeFilter != null
											&& timeVisitor.verify(timeValues[timeIdx]))
									|| (valueFilter != null && timeFilter != null
											&& valueVisitor.verify(v)
											&& timeVisitor.verify(timeValues[timeIdx]))) {
								res.putLong(v);
								res.putTime(timeValues[timeIdx]);
							}
							timeIdx++;
						}

						if (mode == 1) {
							if (update[1].getTime(idx[1]) <= timeValues[timeIdx]
									&& timeValues[timeIdx] <= update[1].getTime(idx[1] + 1)) {
								// do nothing
							} else if ((valueFilter == null && timeFilter == null)
									|| (valueFilter != null && timeFilter == null
											&& valueVisitor.verify(v))
									|| (valueFilter == null && timeFilter != null
											&& timeVisitor.verify(timeValues[timeIdx]))
									|| (valueFilter != null && timeFilter != null
											&& valueVisitor.verify(v)
											&& timeVisitor.verify(timeValues[timeIdx]))) {
								res.putLong(v);
								res.putTime(timeValues[timeIdx]);
							}
							timeIdx++;
						}

						while (mode != -1 && timeIdx < timeValues.length
								&& timeValues[timeIdx] > update[mode].getTime(idx[mode] + 1)) {
							idx[mode] += 2;
							mode = getNextMode(idx[0], idx[1], update[0], update[1]);
						}
					}
					break;
				case FLOAT:
					while (decoder.hasNext(page)) {
						// put insert points
						while (insertTrue.curIdx < insertTrue.length && timeIdx < timeValues.length
								&& insertTrue.getTime(insertTrue.curIdx) <= timeValues[timeIdx]) {
							res.putTime(insertTrue.getTime(insertTrue.curIdx));
							res.putFloat(insertTrue.getFloat(insertTrue.curIdx));
							insertTrue.curIdx++;
							res.insertTrueIndex++;
							calculateFrequency(hasOverflowDataInThisPage, freqFilter,
									insertTrue.getFloat(insertTrue.curIdx));
							// if equal, take value from insertTrue and skip one
							// value from page
							if (insertTrue.getTime(insertTrue.curIdx - 1) == timeValues[timeIdx]) {
								timeIdx++;
								decoder.readFloat(page);
								if (!decoder.hasNext(page)) {
									break;
								}
							}
						}
						if (!decoder.hasNext(page)) {
							break;
						}
						float v = decoder.readFloat(page);
						calculateFrequency(hasOverflowDataInThisPage, freqFilter, v);
						if (mode == -1) {
							if ((valueFilter == null && timeFilter == null)
									|| (valueFilter != null && timeFilter == null
											&& valueVisitor.verify(v))
									|| (valueFilter == null && timeFilter != null
											&& timeVisitor.verify(timeValues[timeIdx]))
									|| (valueFilter != null && timeFilter != null
											&& valueVisitor.verify(v)
											&& timeVisitor.verify(timeValues[timeIdx]))) {
								res.putFloat(v);
								res.putTime(timeValues[timeIdx]);
							}
							timeIdx++;
						}

						if (mode == 0) {
							if (update[0].getTime(idx[0]) <= timeValues[timeIdx]
									&& timeValues[timeIdx] <= update[0].getTime(idx[0] + 1)) {
								// update the value
								if (timeFilter == null
										|| timeVisitor.verify(timeValues[timeIdx])) {
									res.putFloat(update[0].getFloat(idx[0] / 2));
									res.putTime(timeValues[timeIdx]);
								}
							} else if ((valueFilter == null && timeFilter == null)
									|| (valueFilter != null && timeFilter == null
											&& valueVisitor.verify(v))
									|| (valueFilter == null && timeFilter != null
											&& timeVisitor.verify(timeValues[timeIdx]))
									|| (valueFilter != null && timeFilter != null
											&& valueVisitor.verify(v)
											&& timeVisitor.verify(timeValues[timeIdx]))) {
								res.putFloat(v);
								res.putTime(timeValues[timeIdx]);
							}
							timeIdx++;
						}

						if (mode == 1) {
							if (update[1].getTime(idx[1]) <= timeValues[timeIdx]
									&& timeValues[timeIdx] <= update[1].getTime(idx[1] + 1)) {
								// do nothing
							} else if ((valueFilter == null && timeFilter == null)
									|| (valueFilter != null && timeFilter == null
											&& valueVisitor.verify(v))
									|| (valueFilter == null && timeFilter != null
											&& timeVisitor.verify(timeValues[timeIdx]))
									|| (valueFilter != null && timeFilter != null
											&& valueVisitor.verify(v)
											&& timeVisitor.verify(timeValues[timeIdx]))) {
								res.putFloat(v);
								res.putTime(timeValues[timeIdx]);
							}
							timeIdx++;
						}

						while (mode != -1 && timeIdx < timeValues.length
								&& timeValues[timeIdx] > update[mode].getTime(idx[mode] + 1)) {
							idx[mode] += 2;
							mode = getNextMode(idx[0], idx[1], update[0], update[1]);
						}
					}
					break;
				case DOUBLE:
					while (decoder.hasNext(page)) {
						// put insert points
						while (insertTrue.curIdx < insertTrue.length && timeIdx < timeValues.length
								&& insertTrue.getTime(insertTrue.curIdx) <= timeValues[timeIdx]) {
							res.putTime(insertTrue.getTime(insertTrue.curIdx));
							res.putDouble(insertTrue.getDouble(insertTrue.curIdx));
							insertTrue.curIdx++;
							res.insertTrueIndex++;
							calculateFrequency(hasOverflowDataInThisPage, freqFilter,
									insertTrue.getDouble(insertTrue.curIdx));
							// if equal, take value from insertTrue and skip one
							// value from page
							if (insertTrue.getTime(insertTrue.curIdx - 1) == timeValues[timeIdx]) {
								timeIdx++;
								decoder.readDouble(page);
								if (!decoder.hasNext(page)) {
									break;
								}
							}
						}
						if (!decoder.hasNext(page)) {
							break;
						}
						double v = decoder.readDouble(page);
						calculateFrequency(hasOverflowDataInThisPage, freqFilter, v);
						if (mode == -1) {
							if ((valueFilter == null && timeFilter == null)
									|| (valueFilter != null && timeFilter == null
											&& valueVisitor.verify(v))
									|| (valueFilter == null && timeFilter != null
											&& timeVisitor.verify(timeValues[timeIdx]))
									|| (valueFilter != null && timeFilter != null
											&& valueVisitor.verify(v)
											&& timeVisitor.verify(timeValues[timeIdx]))) {
								res.putDouble(v);
								res.putTime(timeValues[timeIdx]);
							}
							timeIdx++;
						}

						if (mode == 0) {
							if (update[0].getTime(idx[0]) <= timeValues[timeIdx]
									&& timeValues[timeIdx] <= update[0].getTime(idx[0] + 1)) {
								// update the value
								if (timeFilter == null
										|| timeVisitor.verify(timeValues[timeIdx])) {
									res.putDouble(update[0].getDouble(idx[0] / 2));
									res.putTime(timeValues[timeIdx]);
								}
							} else if ((valueFilter == null && timeFilter == null)
									|| (valueFilter != null && timeFilter == null
											&& valueVisitor.verify(v))
									|| (valueFilter == null && timeFilter != null
											&& timeVisitor.verify(timeValues[timeIdx]))
									|| (valueFilter != null && timeFilter != null
											&& valueVisitor.verify(v)
											&& timeVisitor.verify(timeValues[timeIdx]))) {
								res.putDouble(v);
								res.putTime(timeValues[timeIdx]);
							}
							timeIdx++;
						}

						if (mode == 1) {
							if (update[1].getTime(idx[1]) <= timeValues[timeIdx]
									&& timeValues[timeIdx] <= update[1].getTime(idx[1] + 1)) {
								// do nothing
							} else if ((valueFilter == null && timeFilter == null)
									|| (valueFilter != null && timeFilter == null
											&& valueVisitor.verify(v))
									|| (valueFilter == null && timeFilter != null
											&& timeVisitor.verify(timeValues[timeIdx]))
									|| (valueFilter != null && timeFilter != null
											&& valueVisitor.verify(v)
											&& timeVisitor.verify(timeValues[timeIdx]))) {
								res.putDouble(v);
								res.putTime(timeValues[timeIdx]);
							}
							timeIdx++;
						}

						while (mode != -1 && timeIdx < timeValues.length
								&& timeValues[timeIdx] > update[mode].getTime(idx[mode] + 1)) {
							idx[mode] += 2;
							mode = getNextMode(idx[0], idx[1], update[0], update[1]);
						}
					}
					break;
				default:
					throw new IOException("Data type not support. " + dataType);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			// Don't forget to update the curIdx in updateTrue and updateFalse
			update[0].curIdx = idx[0];
			update[1].curIdx = idx[1];
			return res;
		}

		// TODO 有一个bug，没有考虑删除的东西
		public boolean checkDataChanged(long mint, long maxt, DynamicOneColumnData updateTrue, int idx0,
				DynamicOneColumnData updateFalse, int idx1, DynamicOneColumnData insertTrue, int idx2,
				SingleSeriesFilterExpression timeFilter) {
			// Judge whether updateTrue has value for this page.
			while (idx0 <= updateTrue.timeLength - 2) {
				if (!((updateTrue.getTime(idx0 + 1) < mint) || (updateTrue.getTime(idx0) > maxt))) {
					return true;
				}
				idx0 += 2;
			}

			while (idx1 <= updateFalse.timeLength - 2) {
				if (!((updateFalse.getTime(idx1 + 1) < mint) || (updateFalse.getTime(idx1) > maxt))) {
					return true;
				}
				idx1 += 2;
			}

			while (idx2 <= insertTrue.length - 1) {
				if (mint <= insertTrue.getTime(idx2) && insertTrue.getTime(idx2) <= maxt) {
					return true;
				}
				if (maxt < insertTrue.getTime(idx2)){
					break;
				}
				if (insertTrue.length > 0 && mint > insertTrue.getTime(insertTrue.length - 1) ){
					break;
				}
				idx2++;
			}
			return false;
		}
		
		// TODO 有一个bug，没有考虑删除的东西
		public boolean checkDataChangedForAggregation(long mint, long maxt, DigestForFilter pageDigest, DynamicOneColumnData updateTrue, int idx0,
				DynamicOneColumnData updateFalse, int idx1, DynamicOneColumnData insertTrue, int idx2,
				SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter) {
			if(checkDataChanged(mint, maxt, updateTrue, idx0, updateFalse, idx1, insertTrue, idx2, timeFilter)){
				return true;
			}
			
			DigestForFilter timeDigest = new DigestForFilter(mint, maxt);
			PageAllSatisfiedVisitor visitor = new PageAllSatisfiedVisitor();
			if(timeFilter != null && !visitor.satisfy(timeDigest, timeFilter)){
				return true;
			}
			if(valueFilter != null && !visitor.satisfy(pageDigest, valueFilter)){
				return true;
			}
			return false;
		}

		public void calculateFrequency(boolean hasOverflowDataInThisPage, SingleSeriesFilterExpression freqFilter, int v) {
			
		}

		public void calculateFrequency(boolean hasOverflowDataInThisPage, SingleSeriesFilterExpression freqFilter, long v) {
			
		}

		public void calculateFrequency(boolean hasOverflowDataInThisPage, SingleSeriesFilterExpression freqFilter, float v) {
			
		}

		public void calculateFrequency(boolean hasOverflowDataInThisPage, SingleSeriesFilterExpression freqFilter, double v) {
			
		}

	
}
