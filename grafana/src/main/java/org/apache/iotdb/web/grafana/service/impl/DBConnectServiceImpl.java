package org.apache.iotdb.web.grafana.service.impl;

import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.web.grafana.bean.TimeValues;
import org.apache.iotdb.web.grafana.dao.BasicDao;
import org.apache.iotdb.web.grafana.service.DBConnectService;

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
