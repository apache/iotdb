package cn.edu.tsinghua.iotdb.exception;

public class StartupException extends Exception{
    private static final long serialVersionUID = -8591716406230730147L;

    public StartupException(String msg){
	super(msg);
    }
    
    public StartupException(String msg, Throwable cause){
	super(msg, cause);
    }
}
