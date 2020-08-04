package org.apache.iotdb.db.engine.tsfilemanagement.normal;

import static org.apache.iotdb.db.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.tsfilemanagement.TsFileManagement;

public class NormalTsFileManagement extends TsFileManagement {

  // includes sealed and unsealed sequence TsFiles
  private TreeSet<TsFileResource> sequenceFileTreeSet = new TreeSet<>(
      (o1, o2) -> {
        int rangeCompare = Long.compare(Long.parseLong(o1.getTsFile().getParentFile().getName()),
            Long.parseLong(o2.getTsFile().getParentFile().getName()));
        return rangeCompare == 0 ? compareFileName(o1.getTsFile(), o2.getTsFile()) : rangeCompare;
      });

  // includes sealed and unsealed unSequence TsFiles
  private List<TsFileResource> unSequenceFileList = new ArrayList<>();

  public NormalTsFileManagement() {
  }

  // ({systemTime}-{versionNum}-{mergeNum}.tsfile)
  private int compareFileName(File o1, File o2) {
    String[] items1 = o1.getName().replace(TSFILE_SUFFIX, "")
        .split(FILE_NAME_SEPARATOR);
    String[] items2 = o2.getName().replace(TSFILE_SUFFIX, "")
        .split(FILE_NAME_SEPARATOR);
    long ver1 = Long.parseLong(items1[0]);
    long ver2 = Long.parseLong(items2[0]);
    int cmp = Long.compare(ver1, ver2);
    if (cmp == 0) {
      return Long.compare(Long.parseLong(items1[1]), Long.parseLong(items2[1]));
    } else {
      return cmp;
    }
  }
}
