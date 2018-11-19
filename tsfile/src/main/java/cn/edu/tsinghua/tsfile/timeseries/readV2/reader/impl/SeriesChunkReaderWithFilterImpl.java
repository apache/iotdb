package cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl;

import cn.edu.tsinghua.tsfile.common.constant.StatisticConstant;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.StrDigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.TimeValuePairFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.impl.DigestFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.impl.TimeValuePairFilterVisitorImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;

import java.io.InputStream;

/**
 * Created by zhangjinrui on 2017/12/24.
 */
public class SeriesChunkReaderWithFilterImpl extends SeriesChunkReader {

    private Filter<?> filter;
    private DigestFilterVisitor digestFilterVisitor;
    private TimeValuePairFilterVisitor<Boolean> timeValuePairFilterVisitor;

    public SeriesChunkReaderWithFilterImpl(InputStream seriesChunkInputStream, TSDataType dataType,
                                           CompressionTypeName compressionTypeName, Filter<?> filter) {
        super(seriesChunkInputStream, dataType, compressionTypeName);
        this.filter = filter;
        this.timeValuePairFilterVisitor = new TimeValuePairFilterVisitorImpl();
        this.digestFilterVisitor = new DigestFilterVisitor();
    }

    @Override
    public boolean pageSatisfied(PageHeader pageHeader) {
        if (pageHeader.data_page_header.max_timestamp < getMaxTombstoneTime())
            return false;
        DigestForFilter timeDigest = new DigestForFilter(pageHeader.data_page_header.getMin_timestamp(),
                pageHeader.data_page_header.getMax_timestamp());
        //TODO: Using ByteBuffer as min/max is best
        DigestForFilter valueDigest = new DigestForFilter(
                pageHeader.data_page_header.digest.getStatistics().get(StatisticConstant.MIN_VALUE),
                pageHeader.data_page_header.digest.getStatistics().get(StatisticConstant.MAX_VALUE),
                dataType);
        return digestFilterVisitor.satisfy(timeDigest, valueDigest, filter);
    }

    @Override
    public boolean timeValuePairSatisfied(TimeValuePair timeValuePair) {
        if (timeValuePair.getTimestamp() < getMaxTombstoneTime())
            return false;
        return timeValuePairFilterVisitor.satisfy(timeValuePair, filter);
    }
}
