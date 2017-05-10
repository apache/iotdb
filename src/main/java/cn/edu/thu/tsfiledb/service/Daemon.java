package cn.edu.thu.tsfiledb.service;

import java.io.IOException;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import cn.edu.thu.tsfiledb.auth.dao.DBdao;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSIService;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSIService.Processor;


public class Daemon {
    
	public static void main(String[] args) {
		//init Derby Database
        DBdao dBdao = new DBdao();
		dBdao.open();
        
        startRPC();
	}
	
	public static TServer startRPC() {
	       try {
	            // 设置服务器端口
	            TServerSocket serverTransport = new TServerSocket(JDBCServerConfig.PORT);
	            // 设置二进制协议工厂
	            Factory protocolFactory = new TBinaryProtocol.Factory();
	            //处理器关联业务实现
	            Processor<TSIService.Iface> processor = new TSIService.Processor<TSIService.Iface>(new TSServiceImpl());
	            // 2. 使用线程池服务模型
	            TThreadPoolServer.Args poolArgs = new TThreadPoolServer.Args(serverTransport);
	            poolArgs.processor(processor);
	            poolArgs.protocolFactory(protocolFactory);
	            TServer poolServer = new TThreadPoolServer(poolArgs);
	            poolServer.serve();

	        } catch (TTransportException e) {

	            e.printStackTrace();

	        } catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	       
	       return null;
	}
    
}
