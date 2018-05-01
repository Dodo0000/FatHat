package org.fathat.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.fathat.datasource.DataSource;

/**
 * @author wyhong
 * @date 2018-4-28
 */
public class PropertiesUtil {

	public static Properties getProperties(String path){
		Properties properties = new Properties();
		InputStream inputStream = PropertiesUtil.class.getClassLoader().getResourceAsStream(path);
		try {
			properties.load(inputStream);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return properties;
	}
}
