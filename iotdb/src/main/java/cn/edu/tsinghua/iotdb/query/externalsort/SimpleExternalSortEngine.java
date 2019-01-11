//package cn.edu.tsinghua.iotdb.query.externalsort;
//
//import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
//import cn.edu.tsinghua.iotdb.query.reader.merge.PrioritySeriesReader;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
//
//public class SimpleExternalSortEngine implements ExternalSortJobEngine {
//
//    private ExternalSortJobScheduler scheduler;
//    //TODO: using config
//    private String baseDir;
//    private int minExternalSortSourceCount;
//
//    private SimpleExternalSortEngine() {
//        baseDir = TsfileDBDescriptor.getInstance().getConfig().readTmpFileDir + File.separatorChar;
//        minExternalSortSourceCount = TsfileDBDescriptor.getInstance().getConfig().externalSortThreshold;
//        scheduler = ExternalSortJobScheduler.getInstance();
//    }
//
//    public SimpleExternalSortEngine(String baseDir, int minExternalSortSourceCount) {
//        this.baseDir = baseDir;
//        this.minExternalSortSourceCount = minExternalSortSourceCount;
//        scheduler = ExternalSortJobScheduler.getInstance();
//    }
//
//    @Override
//    public List<PrioritySeriesReader> executeWithGlobalTimeFilter(List<PrioritySeriesReader> readers) throws IOException {
//        if (readers.size() < minExternalSortSourceCount) {
//            return readers;
//        }
//        ExternalSortJob job = createJob(readers);
//        return job.executeWithGlobalTimeFilter();
//    }
//
//    //TODO: this method could be optimized to have a better performance
//    @Override
//    public ExternalSortJob createJob(List<PrioritySeriesReader> readers) {
//        long jodId = scheduler.genJobId();
//        List<ExternalSortJobPart> ret = new ArrayList<>();
//        List<ExternalSortJobPart> tmpPartList = new ArrayList<>();
//        for (PrioritySeriesReader reader : readers) {
//            ret.add(new SingleSourceExternalSortJobPart(reader));
//        }
//
//        int partId = 0;
//        while (ret.size() >= minExternalSortSourceCount) {
//            for (int i = 0; i < ret.size(); ) {
//                List<ExternalSortJobPart> partGroup = new ArrayList<>();
//                for (int j = 0; j < minExternalSortSourceCount && i < ret.size(); j++) {
//                    partGroup.add(ret.get(i));
//                    i++;
//                }
//                StringBuilder tmpFilePath = new StringBuilder(baseDir).append(jodId).append("_").append(partId);
//                MultiSourceExternalSortJobPart part = new MultiSourceExternalSortJobPart(tmpFilePath.toString(), partGroup);
//                tmpPartList.add(part);
//                partId++;
//            }
//            ret = tmpPartList;
//            tmpPartList = new ArrayList<>();
//        }
//        return new ExternalSortJob(jodId, ret);
//    }
//
//    private static class SimpleExternalSortJobEngineHelper {
//        private static SimpleExternalSortEngine INSTANCE = new SimpleExternalSortEngine();
//    }
//
//    public static SimpleExternalSortEngine getInstance() {
//        return SimpleExternalSortJobEngineHelper.INSTANCE;
//    }
//}
