package org.apache.iotdb.db.tools;

import org.apache.iotdb.tsfile.read.common.RowRecord;

/**
 * Created by wangyihan on 2019/6/3 12:00 AM.
 * E-mail address is yihanwang22@163.com.
 * Copyright © 2017 wangyihan. All Rights Reserved.
 *
 * @author wangyihan
 */
public interface WatermarkEncoder {
  RowRecord encodeRecord(RowRecord record);
}
