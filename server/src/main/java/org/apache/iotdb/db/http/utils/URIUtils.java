package org.apache.iotdb.db.http.utils;

import org.apache.iotdb.db.http.constant.HttpConstant;

public class URIUtils {
  public static String removeParameter(String uri) {
    int index = uri.indexOf(HttpConstant.QUESTION_MARK);
    if(index < 0) {
      if(uri.charAt(uri.length() - 1) == '/') {
        return uri.substring(0, uri.length() - 1);
      }
    } else {
      return uri.substring(0, index);
    }
    return uri;
  }
}
