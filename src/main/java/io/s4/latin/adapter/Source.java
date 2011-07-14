package io.s4.latin.adapter;

import io.s4.latin.core.ObjectUtil;
import io.s4.listener.EventHandler;
import io.s4.listener.EventProducer;

public class Source implements ISource, EventProducer {

	private ISource object;

	public Source(String className, String properties){
		Object o = ObjectUtil.construct(className, properties);
		if (o instanceof ISource) {
			object = (ISource) o;
		}
		
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
