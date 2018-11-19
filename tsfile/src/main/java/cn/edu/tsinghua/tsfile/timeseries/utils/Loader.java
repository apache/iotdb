package cn.edu.tsinghua.tsfile.timeseries.utils;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

public class Loader {
	public static Set<URL> getResources(String resource, ClassLoader classLoader) throws IOException{
		Set<URL> urlSet = new HashSet<>();
		Enumeration<URL> urlEnum = classLoader.getResources(resource);
		while(urlEnum.hasMoreElements()){
			urlSet.add(urlEnum.nextElement());
		}
		return urlSet;
	}
	
	public static URL getResource(String resource, ClassLoader classLoader){
		return classLoader.getResource(resource);
	}
	
	public static ClassLoader getClassLoaderOfObject(Object o){
		if(o == null){
			throw new NullPointerException("Input object cannot be null");
		}
		return getClassLoaderOfClass(o.getClass());
	}
	
	public static ClassLoader getClassLoaderOfClass(final Class<?> clazz){
		ClassLoader classLoader = clazz.getClassLoader();
		return classLoader == null ? ClassLoader.getSystemClassLoader() : classLoader;
	}
}
