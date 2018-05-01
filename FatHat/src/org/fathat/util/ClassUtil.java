package org.fathat.util;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * @author wyhong
 * @date 2018-4-28
 */
//注意点：
//①windows路径“\”要转义一下
//②路径如果存在空格("%20")要用utf-8解码
//③加载类用"."分隔包，结尾是类名

/** 可以修改成扫描所有子包的情况 */
public class ClassUtil {

	public static List<Class> getAllDao() {
		// System.out.println(new
		// File("C:\\Users\\Administrator\\Workspaces\\MyEclipse%2010\\FatHat\\bin\\org\\fathat\\dao\\EnterpriseDao.class").exists());==>false
		// System.out.println(new
		// File("C:\\Users\\Administrator\\Workspaces\\MyEclipse 10\\FatHat\\bin\\org\\fathat\\dao\\EnterpriseDao.class").exists());==>true
		String basePackage = null;
		basePackage = PropertiesUtil.getProperties("db.properties")
				.getProperty("dao.basePackage");
		File file = null;
		try {
			file = new File(URLDecoder.decode(ClassUtil.class.getClassLoader()
					.getResource(basePackage.replace(".", "\\")).getPath(),
					"utf-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		// System.out.println(file.getAbsolutePath());

		// 要返回的DAO包 用BFS构造
		List<Class> classList = new ArrayList<Class>();
		Queue<File> fileQue = new LinkedList<File>();
		Queue<String> prefixQue = new LinkedList<String>();
		fileQue.add(file);
		prefixQue.add(basePackage);
		while (!fileQue.isEmpty()) {
			File currentFile = fileQue.poll();
			String currentPrefix = prefixQue.poll();
			if (currentFile.isDirectory()) {
				File[] files = currentFile.listFiles();
				for (File _file : files) {
					try {
						if(_file.isDirectory()){
							fileQue.add(_file);
							prefixQue.add(currentPrefix+"."+_file.getName().split("\\.")[0]);
							continue;
						}
						Class<?> clazz = PropertiesUtil.class
								.getClassLoader()
								.loadClass(
										currentPrefix
												+ "."
												+ _file.getName().split("\\.")[0]);
						classList.add(clazz);
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}
				}
			} else {
				try {
					Class<?> clazz = PropertiesUtil.class
							.getClassLoader()
							.loadClass(
									currentPrefix
											+ "."
											+ file.getName().split("\\.")[0]);
					classList.add(clazz);
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		}
		return classList;
	}
}


//package org.fathat.util;
//
//import java.io.File;
//import java.io.UnsupportedEncodingException;
//import java.net.URLDecoder;
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * @author wyhong
// * @date 2018-4-28
// */
////注意点：
////①windows路径“\”要转义一下
////②路径如果存在空格("%20")要用utf-8解码
////③加载类用"."分隔包，结尾是类名
//public class ClassUtil {
//
//	public static List<Class> getAllDao() {
////		System.out.println(new File("C:\\Users\\Administrator\\Workspaces\\MyEclipse%2010\\FatHat\\bin\\org\\fathat\\dao\\EnterpriseDao.class").exists());==>false
////		System.out.println(new File("C:\\Users\\Administrator\\Workspaces\\MyEclipse 10\\FatHat\\bin\\org\\fathat\\dao\\EnterpriseDao.class").exists());==>true
//		String basePackage = null;
//		basePackage = PropertiesUtil.getProperties("db.properties").getProperty("dao.basePackage");
//		File file = null;
//		try {
//			file = new File(URLDecoder.decode(ClassUtil.class.getClassLoader().getResource(basePackage.replace(".", "\\")).getPath(), "utf-8"));
//		} catch (UnsupportedEncodingException e) {
//			e.printStackTrace();
//		}
////		System.out.println(file.getAbsolutePath());
//		List<Class> classList = new ArrayList<Class>();
//		if(file.isDirectory()){
//			File[] files = file.listFiles();
//			for (File _file : files) {
//				try {
//					Class<?> clazz = PropertiesUtil.class.getClassLoader().loadClass(basePackage+"."+_file.getName().split("\\.")[0]);
//					classList.add(clazz);
//				} catch (ClassNotFoundException e) {
//					e.printStackTrace();
//				}
//			}
//		}
//		return classList;
//	}
//
//}
