package cn.edu.tsinghua.iotdb.engine.filenode;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is used to serialize or deserialize the object T
 * 
 * @author kangrong
 * @author liukun
 * 
 */
public class SerializeUtil<T> {

	private static final Logger LOGGER = LoggerFactory.getLogger(SerializeUtil.class);

	public void serialize(Object obj, String filePath) throws IOException {
		ObjectOutputStream oos = null;
		try {
			oos = new ObjectOutputStream(new FileOutputStream(filePath));
			oos.writeObject(obj);
			oos.flush();
		} catch (IOException e) {
			LOGGER.error("Serizelize the object failed, object {}.", obj);
			throw e;
		} finally {
			if (oos != null) {
				oos.close();
			}
		}
	}

	public Optional<T> deserialize(String filePath) throws IOException {
		ObjectInputStream ois = null;
		File file = new File(filePath);
		if (!file.exists()) {
			return Optional.empty();
		}
		T result = null;
		try {
			ois = new ObjectInputStream(new FileInputStream(file));
			result = (T) ois.readObject();
			ois.close();
		} catch (Exception e) {
			LOGGER.error("Deserialize the object error.");
			if (ois != null) {
				ois.close();
			}
			return Optional.empty();
		}
		return Optional.ofNullable(result);
	}

}
