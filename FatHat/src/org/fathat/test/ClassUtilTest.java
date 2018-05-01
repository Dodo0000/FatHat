package org.fathat.test;

import java.util.List;

import org.fathat.util.ClassUtil;
import org.junit.Test;

/**
 * @author wyhong
 * @date 2018-4-28
 */
public class ClassUtilTest {

	@Test
	public void test(){
		List<Class> clazzList = ClassUtil.getAllDao();
		for(Class clazz : clazzList){
			System.out.println(clazz.getName());
		}
	}
}
