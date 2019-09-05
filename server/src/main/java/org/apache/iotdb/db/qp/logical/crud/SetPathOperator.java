package org.apache.iotdb.db.qp.logical.crud;

import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.tsfile.read.common.Path;

import java.util.ArrayList;
import java.util.List;

public class SetPathOperator extends Operator {
  private List<Path> suffixList;

  public SetPathOperator(int tokenIntType) {
    super(tokenIntType);
    suffixList = new ArrayList<>();
  }

  public void addSelectPath(Path suffixPath) {
    suffixList.add(suffixPath);
  }

  public void setSuffixPathList(List<Path> suffixPaths) {
    suffixList = suffixPaths;
  }

  public List<Path> getSuffixPaths() {
    return suffixList;
  }
}

