package cn.edu.tsinghua.iotdb.utils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import sun.nio.ch.DirectBuffer;

public class CommonUtils {
	public static int javaVersion = CommonUtils.getJDKVersion();
	public static int getJDKVersion() {
    	String[] javaVersionElements = System.getProperty("java.version").split("\\.");
    	if(Integer.parseInt(javaVersionElements[0]) == 1) {
    		return Integer.parseInt(javaVersionElements[1]);
    	} else {
    		return Integer.parseInt(javaVersionElements[0]);
    	}
    }
	
	public static void destroyBuffer(Buffer byteBuffer) throws Exception {
		if (javaVersion == 8) {
    		((DirectBuffer) byteBuffer).cleaner().clean();
    	} else {
    		try {
            	final Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            	final Field theUnsafeField = unsafeClass.getDeclaredField("theUnsafe");
            	theUnsafeField.setAccessible(true);
            	final Object theUnsafe = theUnsafeField.get(null);
            	final Method invokeCleanerMethod = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
            	invokeCleanerMethod.invoke(theUnsafe, byteBuffer);
    		} catch (Exception e) {
    			throw e;
    		}
    	}
	}
}
