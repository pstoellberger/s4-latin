package io.s4.latin.parser;

import io.s4.dispatcher.partitioner.Partitioner;
import io.s4.processor.AbstractPE.FrequencyType;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;


public class LatinParser {

	public LatinParser() {};

	public DefaultListableBeanFactory processPersistDefinitions(String streamLatin, DefaultListableBeanFactory bf) {
		streamLatin = preProcess(streamLatin);
		String[] statements = streamLatin.split("\n");

		int index = 0;
		for (String statement : statements) {
			index++;
			if (statement.toLowerCase().startsWith("persist stream")) {
				String id = "Persister_" + index;
				BeanDefinitionBuilder persistBuilder = BeanDefinitionBuilder.rootBeanDefinition("io.s4.latin.persister.LatinPersisterPE");
				persistBuilder.setScope(BeanDefinition.SCOPE_SINGLETON);
				persistBuilder.addPropertyValue("id", id);
				persistBuilder.addPropertyValue("statement", statement);
				bf.registerBeanDefinition(id, persistBuilder.getBeanDefinition());
			}
		}
		return bf;
	}

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
				sourceBuilder.addConstructorArgValue(getProperties(getSourceClassName(statement),statement)+"\nstream=" + getOutputStreamName(statement));
				sourceBuilder.setInitMethodName("init");
				bf.registerBeanDefinition(id, sourceBuilder.getBeanDefinition());

			}
		}
		return bf;
	}
	public DefaultListableBeanFactory processQueryDefinitions(String queryLatin, DefaultListableBeanFactory bf) {
		queryLatin = preProcess(queryLatin);
		String[] statements = queryLatin.split("\n");

		int index = 0;
		for (String statement : statements) {
			index++;
			String id = getStreamName(statement) + "_" + index;
			if (statement != null && statement.trim().length() > 0 && !statement.toLowerCase().contains("create stream") && !statement.startsWith("persist stream ")) {

					BeanDefinitionBuilder buildPartitioner = BeanDefinitionBuilder.rootBeanDefinition("io.s4.dispatcher.partitioner.DefaultPartitioner");
					buildPartitioner.setScope(BeanDefinition.SCOPE_SINGLETON);
					String[] streamNames = { getStreamName(statement) };

					String[] keys = { "key" };
					buildPartitioner.addPropertyValue("hashKey", keys );
					buildPartitioner.addPropertyValue("streamNames", streamNames );
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

				if (statement.contains("join(")) {
					String className = getJoinClass(statement);
					BeanDefinitionBuilder beanBuildr = BeanDefinitionBuilder.rootBeanDefinition(className);
					beanBuildr.setScope(BeanDefinition.SCOPE_SINGLETON);
					beanBuildr.addPropertyValue("id", id);
					beanBuildr.addPropertyValue("statement", statement);
					beanBuildr.addPropertyReference("dispatcher", "dispatcher"+ getStreamName(statement) + index);
					if (getStreamName(statement).startsWith("debug")) {
						beanBuildr.addPropertyValue("debug", true);
					}
					bf.registerBeanDefinition(id, beanBuildr.getBeanDefinition());

				}
				else if  (statement.contains("process stream")) {
					String className = LatinParser.getUdfClassName(statement);
					String propString = LatinParser.getProperties(className, statement);
					Properties props = new Properties();
					if (propString != null) {
						StringReader sr = new StringReader(propString);
						try {
							props.load(sr);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

					}

					BeanDefinitionBuilder beanBuildr = BeanDefinitionBuilder.rootBeanDefinition(className);
					beanBuildr.setScope(BeanDefinition.SCOPE_SINGLETON);
					beanBuildr.addConstructorArgValue(props);
					beanBuildr.addPropertyValue("outputStreamName", LatinParser.getStreamName(statement));
					beanBuildr.addPropertyValue("id", id);
					beanBuildr.addPropertyReference("dispatcher", "dispatcher"+ getStreamName(statement) + index);
					List<String> streamkeys = new ArrayList<String>();
					streamkeys.add(LatinParser.getProcessStream(statement) + " *");
					System.out.println("Settin key:" + LatinParser.getProcessStream(statement) + " *");
					beanBuildr.addPropertyValue("keys", streamkeys.toArray(new String[1]));

					
					bf.registerBeanDefinition(id, beanBuildr.getBeanDefinition());
				}
				else {
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
		}
		return bf;
	}

	private String preProcess(String latin) {
		if (latin != null) {
			String output = "";
			for (String line : latin.split("\n")) {
				if (!line.startsWith("//") && (line.trim().length() > 0)) {
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
				return statement.substring(iSelect+7,iFrom).replaceAll(" ","").split(",");
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
	
	public static String getPersistFrom(String statement) {
		if (statement != null) {
			String statement2 = statement.toLowerCase();
			int iPersist = statement2.indexOf("persist stream");
			int iTo = statement2.indexOf(" to ", iPersist);
			if (iPersist >= 0) {
				int end = iTo > 0 ? iTo : statement.length();
				return statement.substring(iPersist+"persist stream".length(),end);
			}
		}
		return null;
	}

	public static String getProcessStream(String statement) {
		if (statement != null) {
			String statement2 = statement.toLowerCase();
			int iPersist = statement2.indexOf("process stream");
			int iTo = statement2.indexOf(" with ", iPersist);
			if (iPersist >= 0) {
				int end = iTo > 0 ? iTo : statement.length();
				return statement.substring(iPersist+"process stream ".length(),end);
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
	
	public static String getUdfClassName(String statement) {
		if (statement != null) {
			int start = statement.toLowerCase().indexOf("udf(");
			int end = statement.indexOf(",",start);
			if (start >= 0 && end >= 0) {
				return statement.substring(start+"UDF(".length(), end);
			}
		}
		throw new RuntimeException("Cannot parse statement for source class name: " + statement);
	}


	public static String getOutputClassName(String statement) {
		if (statement != null) {
			int start = statement.toLowerCase().indexOf("output(");
			int end = statement.indexOf(",",start);
			if (start >= 0 && end >= 0) {
				return statement.substring(start+"output(".length(), end);
			}
		}
		throw new RuntimeException("Cannot parse statement for output class name: " + statement);
	}

	public static FrequencyType getOutputType(String statement) {
		if (statement != null && !statement.trim().endsWith(")")) {
			int tstart = statement.toLowerCase().indexOf("output(");
			int tend = statement.indexOf(")",tstart);
			if (tstart >= 0 && tend >= 0) {
				String interval =  statement.substring(tend, statement.length());
				String[] tokens = interval.split(" ");
				if (tokens.length == 3 && tokens[0].equals("every")) {
					if (tokens[3].equals("seconds")) {
						return FrequencyType.TIMEBOUNDARY;
					}
					else if (tokens[3].equals("events")) {
						return FrequencyType.EVENTCOUNT;
					}
					else {
						throw new RuntimeException("Cannot parse output type: " + tokens[3]);
					}
				}
			}

		}
		return null;
	}

	public static Integer getOutputFrequency(String statement) {
		if (statement != null && !statement.trim().endsWith(")")) {
			int tstart = statement.toLowerCase().indexOf("output(");
			int tend = statement.indexOf(")",tstart);
			if (tstart >= 0 && tend >= 0) {
				String interval =  statement.substring(tend, statement.length());
				String[] tokens = interval.split(" ");
				if (tokens.length == 3 && tokens[0].equals("every")) {
					Integer frequency = Integer.parseInt(tokens[2]);
					return frequency;
				}
			}

		}
		throw new RuntimeException("Cannot parse statement for output frequency: " + statement);
	}

	public static String[] getJoinKeys(String statement) {
		if (statement != null) {
			int iSelect = statement.toLowerCase().indexOf("join( ");
			int endclass = statement.indexOf(") on",iSelect);
			int iFrom = statement.toLowerCase().indexOf(" include ");
			if (endclass >= 0 && iFrom >= 0) {
				String[] keys = statement.substring(endclass+") on".length(),iFrom).replaceAll(" ","").split(",");
				for (int i=0; i < keys.length; i++) {
					keys[i] = keys[i].replace(".", " ");
				}
				return keys;
				
			}
		}
		return null;
	}
	
	public static String[] getJoinIncludes(String statement) {
		if (statement != null) {
			int includestart = statement.toLowerCase().indexOf(" include ");
			int iwindow = statement.toLowerCase().indexOf(" window ");
			if (includestart >= 0) {
				int end = iwindow > 0 ? iwindow : statement.length();
				String keys[] = statement.substring(includestart+" include ".length(),end).replaceAll(" ","").split(",");
				for (int i=0; i < keys.length; i++) {
					keys[i] = keys[i].replace(".", " ");
				}
				return keys;
			}
		}
		return null;
	}
	
	public static String getWindow(String statement) {
		if (statement != null) {
			statement = statement.toLowerCase();
			int iWhere = statement.indexOf(" window ");
			if (iWhere >= 0) {
				return statement.substring(iWhere+" window ".length(),statement.length());
			}
		}
		return null;
	}
	
	public static String getJoinClass(String statement) {
		String stmt = statement.toLowerCase();
		int start = statement.indexOf(" join(");
		int end = statement.indexOf(")",start);

		if (start >= 0 && end >= 0) {
			String className  = statement.substring(start+" join(".length(), end);
			return className;
		}
	    return null;
	}
	
	public static String getProperties(String className, String statement) {
		if (statement != null) {
			int start = statement.indexOf(className);
			int end = statement.indexOf(")",start);
			if (start >= 0 && end >= 0) {
				String props = statement.substring(start+className.length()+1, end);
				props = props.replaceAll(";", "\n").replaceAll(" ", "");
				return props;
			}
		}
		throw new RuntimeException("Cannot parse statement for properties: " + statement);
	}


public static void main(String[] args) {
	System.out.println(getJoinClass("joined = join(io.s4.latin.core.LatinJoinPE) on speech.id,sentences.speechId include speech.*,sentences.*"));
}
}
