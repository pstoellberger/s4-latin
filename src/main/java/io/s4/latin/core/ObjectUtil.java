package io.s4.latin.core;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.util.Properties;

import org.apache.log4j.Logger;

public class ObjectUtil {

	public static Object construct(String className, String properties) {
		Properties props = new Properties();
		if (properties != null) {
			StringReader sr = new StringReader(properties);
			try {
				props.load(sr);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		Object[] propsArgs = { props };
		Class propsArgClass = Properties.class;
		Constructor propsArgConstructor;
		Class sourceDefinition;

		try {
			sourceDefinition = Class.forName(className);
			propsArgConstructor = sourceDefinition.getConstructor(propsArgClass);
			return createObject(propsArgConstructor, propsArgs);

		} catch (Exception e) {
			e.printStackTrace();
			Logger.getLogger("s4").error(e);
		}
		return null;

	}

	protected static Object createObject(Constructor constructor, Object[] arguments) throws Exception {

		System.out.println("Constructor: " + constructor.toString());
		Object object = null;

		object = constructor.newInstance(arguments);
		System.out.println("Object: " + object.toString());
		return object;
	}


}
