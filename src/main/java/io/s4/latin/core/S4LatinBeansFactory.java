package io.s4.latin.core;

import io.s4.latin.parser.LatinParser;

import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ApplicationContext;

public class S4LatinBeansFactory {

	public static DefaultListableBeanFactory createPEs(ApplicationContext parent, String filecontent) {
		DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory(parent);
		LatinParser lp = new LatinParser();
		beanFactory = lp.processQueryDefinitions(filecontent,beanFactory);
		beanFactory = lp.processPersistDefinitions(filecontent, beanFactory);
		return beanFactory;
	}

	public static DefaultListableBeanFactory createAdapters(ApplicationContext parent, String filecontent) {
		DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory(parent);
		LatinParser lp = new LatinParser();
		beanFactory = lp.processStreamdefinitions(filecontent,beanFactory);
		return beanFactory;
	}

}
