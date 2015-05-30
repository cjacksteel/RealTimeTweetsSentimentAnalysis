package edu.pennstate.greatvalley.sweng.storm.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AppConfig {

	private static Properties config  = null;
	
	private static final Logger logger = LoggerFactory.getLogger(AppConfig.class);
	
	private AppConfig() {
		
		config = new Properties();
		
		InputStream inputStream = getClass().getClassLoader().getResourceAsStream(AppConstant.APP_CONFIG_FILE_NAME);
	
		if (inputStream != null) {
			try {
				config.load(inputStream);
			} catch (IOException e) {
				logger.error("Could not load properties : ");
				logger.error(e.getMessage());
			}
		}
	
	}
	
	public static String getPropertyValue(String propertyName) {
		if (config == null) {
			new AppConfig();
		}
		
		return config.getProperty(propertyName);
		
	}
}
