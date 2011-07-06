package io.s4.latin.adapter;

import io.s4.listener.EventHandler;
import io.s4.listener.EventProducer;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.util.Properties;

public class Source implements ISource, EventProducer {

	private ISource object;

	public Source(String className, String properties){
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
			this.object = (ISource) createObject(propsArgConstructor, propsArgs);
		} catch (ClassNotFoundException e) {
			System.out.println(e);
		} catch (NoSuchMethodException e) {
			System.out.println(e);
		}


	}

	protected static Object createObject(Constructor constructor,
			Object[] arguments) {

		System.out.println("Constructor: " + constructor.toString());
		Object object = null;

		try {
			object = constructor.newInstance(arguments);
			System.out.println("Object: " + object.toString());
			return object;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return object;
	}

	@Override
	public void addHandler(EventHandler arg0) {
		object.addHandler(arg0);
		
	}

	@Override
	public boolean removeHandler(EventHandler arg0) {
		return object.removeHandler(arg0);
	}

	@Override
	public void init() {
		this.object.init();
		
	}
	

}
