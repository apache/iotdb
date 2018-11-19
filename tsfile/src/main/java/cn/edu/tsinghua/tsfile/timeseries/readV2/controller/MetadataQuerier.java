package cn.edu.tsinghua.tsfile.timeseries.readV2.controller;

import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public interface MetadataQuerier {

    List<EncodedSeriesChunkDescriptor> getSeriesChunkDescriptorList(Path path) throws IOException;

}
