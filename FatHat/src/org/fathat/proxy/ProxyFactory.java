package org.fathat.proxy;

import java.lang.reflect.Proxy;

/**
 * @author wyhong
 * @date 2018-4-28
 */
public class ProxyFactory {

	public static Object getDaoProxy(Class<?> targetClass, DaoProxy handler) {
		Class[] c = {targetClass};
		return Proxy.newProxyInstance(targetClass.getClassLoader(), c, handler);
	}

}
