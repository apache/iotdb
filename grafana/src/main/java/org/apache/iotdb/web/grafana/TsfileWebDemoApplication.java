package org.apache.iotdb.web.grafana;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TsfileWebDemoApplication {

	public static void main(String[] args) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
		SpringApplication.run(TsfileWebDemoApplication.class, args);
	}
}
