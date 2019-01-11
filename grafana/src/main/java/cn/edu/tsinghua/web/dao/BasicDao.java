package cn.edu.tsinghua.web.dao;

import cn.edu.tsinghua.tsfile.utils.Pair;
import cn.edu.tsinghua.web.bean.TimeValues;

import java.time.ZonedDateTime;
import java.util.List;

/**
 * Created by dell on 2017/7/17.
 */
public interface BasicDao {

    List<TimeValues> querySeries(String s, Pair<ZonedDateTime, ZonedDateTime> timeRange);

    List<String> getMetaData();

}
