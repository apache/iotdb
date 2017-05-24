package cn.edu.thu.tsfiledb.engine.filenode;

import java.io.File;

public class Mi {

	public static void main(String[] args) {
		
//		String path = "/user/user/name";
//		String[] paths = path.split(File.separator);
//		System.out.println(paths.length);
//		for(String data :paths){
//			System.out.println(data);
//		}
		// TODO Auto-generated method stub
		String path = "fdafad\\fds";
		System.out.println("\\"+File.separator);
		System.out.println("/");
		System.out.println("\");
		String[] paths = path.split("\\"+File.separator);
		for(String str:paths){
			System.out.println(str);
		}
		System.out.println(File.separator);
		System.out.println(File.pathSeparator);
		
		System.out.println(File.pathSeparatorChar);
		System.out.println(File.separatorChar);

	}

}
