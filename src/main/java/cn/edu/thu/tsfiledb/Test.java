package cn.edu.thu.tsfiledb;

import cn.edu.thu.tsfiledb.engine.filenode.FileNodeManager;

public class Test {

	public static void main(String[] args) {
		myThread thread = new myThread();
		thread.start();
		FileNodeManager fileNodeManager = FileNodeManager.getInstance();
		
		System.out.println(fileNodeManager);
	}
	
	public static class myThread extends Thread{
		@Override
		public void run(){
			FileNodeManager fileNodeManager = FileNodeManager.getInstance();
			System.out.println(fileNodeManager);
		}
	}

}
