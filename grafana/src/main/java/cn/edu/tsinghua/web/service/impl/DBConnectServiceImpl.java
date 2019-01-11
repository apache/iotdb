package cn.edu.tsinghua.web.service.impl;

import cn.edu.tsinghua.tsfile.utils.Pair;
import cn.edu.tsinghua.web.bean.TimeValues;
import cn.edu.tsinghua.web.dao.BasicDao;
import cn.edu.tsinghua.web.service.DBConnectService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.ZonedDateTime;
import java.util.List;

/**
 * Created by dell on 2017/7/17.
 */
@Service
public class DBConnectServiceImpl implements DBConnectService {

    @Autowired
    BasicDao basicDao;

    @Override
    public int testConnection() {
        return 0;
    }

    @Override
    public List<TimeValues> querySeries(String s, Pair<ZonedDateTime, ZonedDateTime> timeRange) {
        return basicDao.querySeries(s, timeRange);
    }

    @Override
    public List<String> getMetaData() {
        return basicDao.getMetaData();
    }

}
