package org.fathat.proxy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.fathat.annotation.Dao;
import org.fathat.util.ClassUtil;

/**
 * @author wyhong
 * @date 2018-4-28
 */
public class DaoContainer {
	
	private static Map<String, Object> daoMapper = new HashMap<String, Object>();
	
	static{
		List<Class> classList = ClassUtil.getAllDao();
		for(Class clazz : classList) {
			//生成代理对象
			//要保证注解运行时存在
			if(clazz.isAnnotationPresent(Dao.class)){
//				System.out.println(clazz.getName()+":annotation presents");
				Dao dao = (Dao)clazz.getAnnotation(Dao.class);
				String daoName = dao.value();
				if(daoName == null){
					int unicode = (int)clazz.getName().charAt(0);
					daoName = (char)(unicode + 32) + clazz.getName().substring(1);
				}
//				System.out.println(daoName);
				daoMapper.put(daoName, ProxyFactory.getDaoProxy(clazz, new DaoProxy()));
			}
		}
	}
	
	public static Object getDao(String daoName){
		return daoMapper.get(daoName);
	}
	
}
