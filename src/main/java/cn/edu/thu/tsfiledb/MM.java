package cn.edu.thu.tsfiledb;

import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.metadata.MManager;
import cn.edu.thu.tsfiledb.metadata.Metadata;

public class MM {

	public static void main(String[] args) throws PathErrorException {
		// TODO Auto-generated method stub
		System.out.println("ff");
        Metadata metadata = MManager.getInstance().getMetadata();
        String metadataInJson = MManager.getInstance().getMetadataInString();
        System.out.println(metadataInJson);
	}

}
