package org.fathat.datasource;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.fathat.util.PropertiesUtil;

/**
 * @author wyhong
 * @date 2018-4-28
 */
public class DataSource {

	private String driverClassName;
	private String url;
	private String username;
	private String password;
	
	public DataSource(String configurationPath){
		Properties properties = PropertiesUtil.getProperties(configurationPath);
		driverClassName = properties.getProperty("jdbc.driverClassName");
		url = properties.getProperty("jdbc.url");
		username = properties.getProperty("jdbc.username");
		password = properties.getProperty("jdbc.password");
	}

	public String getDriverClassName() {
		return driverClassName;
	}

	public String getUrl() {
		return url;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}
	
}
