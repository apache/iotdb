package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.validator;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.io.IOException;
import java.util.List;

public class ResourceAndTsFileDataCorrectnessValidator implements TsFileValidator {

  public static ResourceAndTsFileDataCorrectnessValidator getInstance() {
    return TsFileDataCorrectnessValidatorHolder.INSTANCE;
  }

  @Override
  public boolean validateTsFile(
      TsFileManager manager,
      List<TsFileResource> targetTsFileList,
      String storageGroupName,
      long timePartition,
      boolean isValidateResource)
      throws IOException {
    if (isValidateResource) {
      return CompactionUtils.validateTsFilesCorrectness(targetTsFileList);
    }
    return CompactionUtils.validateTsFileResources(manager, storageGroupName, timePartition)
        && CompactionUtils.validateTsFilesCorrectness(targetTsFileList);
  }

  @Override
  public boolean validateTsFile(TsFileResource tsFileResource) {
    return CompactionUtils.validateSingleTsFileDataCorrectness(tsFileResource);
  }

  private static class TsFileDataCorrectnessValidatorHolder {
    private static final ResourceAndTsFileDataCorrectnessValidator INSTANCE =
        new ResourceAndTsFileDataCorrectnessValidator();
  }
}
