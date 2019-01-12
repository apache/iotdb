package org.apache.iotdb.db.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.iotdb.db.exception.StartupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.iotdb.db.exception.StartupException;

public class RegisterManager {
	private static final Logger LOGGER = LoggerFactory.getLogger(RegisterManager.class);
	private List<IService> iServices;
	public RegisterManager(){
		iServices = new ArrayList<>();
	}
	
	public void register(IService service) throws StartupException {
		for(IService s: iServices){
			if(s.getID() == service.getID()){
				LOGGER.info("{} has already been registered. skip", service.getID().getName());
				return;
			}
		}
		iServices.add(service);
		service.start();
	}
	
	public void deregisterAll(){
		for(IService service: iServices){
			try {
				service.stop();
			} catch (Exception e) {
				LOGGER.error("Failed to stop {} because {}", service.getID().getName(), e.getMessage());
			}
		}
		iServices.clear();
		LOGGER.info("deregister all service.");
	}
}
