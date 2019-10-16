package org.apache.iotdb.tsfile.tool.update;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateTool {

  private static final Logger logger = LoggerFactory.getLogger(UpdateTool.class);

  /**
   * 升级文件夹下所有的tsfile
   *
   * @param dir 文件夹路径
   * @param updateDir 修改后的文件夹
   */
  public static void updateTsfiles(String dir, String updateDir) throws IOException {
    //遍历查找所有的tsfile文件
    File file = new File(dir);
    List<File> tmp = new ArrayList<>();
    tmp.add(file);
    List<String> tsfiles = new ArrayList<>();
    if (file.exists()) {
      while (!tmp.isEmpty()) {
        File tmp_file = tmp.remove(0);
        File[] files = tmp_file.listFiles();
        for (File file2 : files) {
          if (file2.isDirectory()) {
            tmp.add(file2);
          } else {
            if (file2.getName().endsWith(".tsfile")) {
              tsfiles.add(file2.getAbsolutePath());
            }
            if (file2.getName().endsWith(".resource")){
              File newFileName = new File(file2.getAbsoluteFile().toString().replace(dir, updateDir));
              if (!newFileName.getParentFile().exists()){
                newFileName.getParentFile().mkdirs();
              }
              newFileName.createNewFile();
              Files.copy(file2, newFileName);
            }
          }
        }
      }
    }
    //对于每个tsfile文件，进行升级操作
    for (String tsfile : tsfiles) {
      updateOneTsfile(tsfile, tsfile.replace(dir, updateDir));
    }
  }

  /**
   * 升级单个tsfile文件
   *
   * @param tsfileName tsfile的绝对路径
   */
  private static void updateOneTsfile(String tsfileName, String updateFileName) throws IOException {
    TsFileUpdaterV0_8_0 updater = new TsFileUpdaterV0_8_0(tsfileName);
    updater.updateFile(updateFileName);
  }

  public static void main(String[] args) throws IOException {
    List<String> tsfileDirs = new ArrayList<>();
    List<String> tsfileDirsUpdate = new ArrayList<>();
    tsfileDirs.add("/Users/tianyu/2019秋季学期/incubator-iotdb/data/data1");
    tsfileDirsUpdate.add("/Users/tianyu/2019秋季学期/incubator-iotdb/data/data");
    for (int i = 0; i < tsfileDirs.size(); i++) {
      updateTsfiles(tsfileDirs.get(i), tsfileDirsUpdate.get(i));
    }
  }
}