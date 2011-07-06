package io.s4.latin.parser;

import io.s4.dispatcher.partitioner.Partitioner;
import io.s4.latin.adapter.Source;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;


public class LatinParser {

	public LatinParser() {};
	
	public DefaultListableBeanFactory processStreamdefinitions(String streamLatin, DefaultListableBeanFactory bf) {
		streamLatin = preProcess(streamLatin);
		String[] statements = streamLatin.split("\n");

		int index = 0;
		for (String statement : statements) {
			index++;
			if (statement.toLowerCase().contains("create stream")) {
				String id = getOutputStreamName(statement) + "_" + index;
				BeanDefinitionBuilder sourceBuilder = BeanDefinitionBuilder.rootBeanDefinition("io.s4.latin.adapter.Source");
				sourceBuilder.setScope(BeanDefinition.SCOPE_SINGLETON);
				sourceBuilder.addConstructorArgValue(getSourceClassName(statement));
				sourceBuilder.addConstructorArgValue(getProperties(statement)+"\nstream=" + getOutputStreamName(statement));
				sourceBuilder.setInitMethodName("init");
				bf.registerBeanDefinition(id, sourceBuilder.getBeanDefinition());

			}
		}
		return bf;
	}
	public DefaultListableBeanFactory process(String queryLatin, DefaultListableBeanFactory bf) {
		queryLatin = preProcess(queryLatin);
		String[] statements = queryLatin.split("\n");

		int index = 0;
		for (String statement : statements) {
			index++;
			String id = getStreamName(statement) + "_" + index;
			if (!statement.toLowerCase().contains("create stream") && !statement.startsWith("store ")) {
				
				// Partitioner Bean definition
				BeanDefinitionBuilder buildPartitioner = BeanDefinitionBuilder.rootBeanDefinition("io.s4.dispatcher.partitioner.DefaultPartitioner");
				buildPartitioner.setScope(BeanDefinition.SCOPE_SINGLETON);
				String[] streamNames = { getStreamName(statement) };
				String[] keys = { "key" };
				buildPartitioner.addPropertyValue("streamNames", streamNames );
				buildPartitioner.addPropertyValue("hashKey", keys );
				buildPartitioner.addPropertyReference("hasher", "hasher");
				buildPartitioner.addPropertyValue("debug", false);
				bf.registerBeanDefinition("partitioner"+ getStreamName(statement) + index, buildPartitioner.getBeanDefinition());

				// Dispatcher Bean definition
				BeanDefinitionBuilder beanBuildr2 = BeanDefinitionBuilder.rootBeanDefinition("io.s4.dispatcher.Dispatcher");
				beanBuildr2.setInitMethodName("init");
				beanBuildr2.setScope(BeanDefinition.SCOPE_SINGLETON);
				beanBuildr2.addPropertyReference("eventEmitter", "commLayerEmitter");
				beanBuildr2.addPropertyValue("loggerName", "s4");

				bf.registerBeanDefinition("dispatcher"+ getStreamName(statement) + index, beanBuildr2.getBeanDefinition());
				Object pBean = bf.getBean("partitioner"+ getStreamName(statement)+ index);
				List<Partitioner> beanlist = new ArrayList<Partitioner>();
				beanlist.add((Partitioner)pBean);
				beanBuildr2.addPropertyValue("partitioners", beanlist);
	
				// Processing Bean definition
				
				BeanDefinitionBuilder beanBuildr = BeanDefinitionBuilder.rootBeanDefinition("io.s4.latin.core.GenericLatinPE");
				beanBuildr.setScope(BeanDefinition.SCOPE_SINGLETON);
				beanBuildr.addPropertyValue("outputStreamName", getStreamName(statement));
				beanBuildr.addPropertyValue("id", id);
				beanBuildr.addPropertyValue("statement", statement);
				beanBuildr.addPropertyReference("dispatcher", "dispatcher"+ getStreamName(statement) + index);
				if (getStreamName(statement).startsWith("debug")) {
					beanBuildr.addPropertyValue("debug", true);
				}

				bf.registerBeanDefinition(id, beanBuildr.getBeanDefinition());

			}
		}
		return bf;
	}

	private String preProcess(String latin) {
		if (latin != null) {
			String output = "";
			for (String line : latin.split("\n")) {
				if (!line.startsWith("//")) {
					output += line.replace("\n", " ").replaceAll("  ", " ").replaceAll("; ", ";") + "\n";
				}
			}
			return output;
		}
		return null; 
	}

	public static String[] getSelect(String statement) {
		if (statement != null) {
			statement = statement.toLowerCase();
			int iSelect = statement.indexOf("select ");
			int iFrom = statement.indexOf(" from ");
			if (iSelect >= 0 && iFrom >= 0) {
				return statement.substring(iSelect+7,iFrom).split(",");
			}
		}
		return null;
	}

	public static String getFrom(String statement) {
		if (statement != null) {
			String statement2 = statement.toLowerCase();
			int iFrom = statement2.indexOf(" from ");
			int iWhere = statement2.indexOf(" where ");
			if (iFrom >= 0) {
				int end = iWhere > 0 ? iWhere : statement.length();
				return statement.substring(iFrom+6,end);
			}
		}
		return null;
	}

	public static String getWhere(String statement) {
		if (statement != null) {
			statement = statement.toLowerCase();
			int iWhere = statement.indexOf(" where ");
			if (iWhere >= 0) {
				return statement.substring(iWhere+7,statement.length());
			}
		}
		return null;
	}
	public static String getStreamName(String statement) {
		if (statement != null) {
			String[] tokens = statement.split(" ");
			if (tokens.length >= 3 && tokens[1].equals("=")) {
				return tokens[0];
			}
		}
		return null;
	}
	
	public static String getOutputStreamName(String statement) {
		if (statement != null) {
			String[] tokens = statement.split(" ");
			if (tokens.length >= 3 && tokens[0].equals("create") && tokens[1].equals("stream")) {
				return tokens[2];
			}
		}
		return null;
	}
	
	public static String getSourceClassName(String statement) {
		if (statement != null) {
			int start = statement.indexOf("Source(");
			int end = statement.indexOf(",",start);
			if (start >= 0 && end >= 0) {
				return statement.substring(start+"Source(".length(), end);
			}
		}
		throw new RuntimeException("Cannot parse statement for source class name: " + statement);
	}
	
	public static String getProperties(String statement) {
		if (statement != null) {
			int start = statement.indexOf(getSourceClassName(statement));
			int end = statement.indexOf(")",start);
			if (start >= 0 && end >= 0) {
				String props = statement.substring(start+getSourceClassName(statement).length()+1, end);
				props = props.replaceAll(";", "\n");
				return props;
			}
		}
		throw new RuntimeException("Cannot parse statement for properties: " + statement);
	}
	


}
