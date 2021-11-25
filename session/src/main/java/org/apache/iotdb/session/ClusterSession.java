package org.apache.iotdb.session;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RedirectException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletReq;
import org.apache.iotdb.tsfile.write.record.Tablet;

import java.util.ArrayList;
import java.util.List;

public class ClusterSession extends Session{

    List<SessionConnection> sessionConnectionList;
    public ClusterSession(String host, int rpcPort) {
        super(host, rpcPort);
        enableCacheLeader = false;
        sessionConnectionList = new ArrayList<>();
    }

    @Override
    public void insertTablet(Tablet tablet) throws StatementExecutionException, IoTDBConnectionException {
//        TSInsertTabletReq request = genTSInsertTabletReq(tablet, sorted);
//        try {
//            getSessionConnection(tablet.prefixPath).insertTablet(request);
//        } catch (RedirectException e) {
//            handleRedirection(tablet.prefixPath, e.getEndPoint());
//        }
    }

    @Override
    protected SessionConnection getSessionConnection(String deviceId) {
        return null;
    }
}
