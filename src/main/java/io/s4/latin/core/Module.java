package io.s4.latin.core;

import io.s4.listener.EventProducer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import java.util.Map;

import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystemManager;
import org.apache.commons.vfs.VFS;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.core.io.Resource;

public class Module implements ApplicationContextAware {

	private ApplicationContext ctx;
	private List<String> latinFileLocation;
	private boolean processAdapters = false;
	private boolean processPEs = false;

	public void setProcessPEs(boolean processPEs) {
		this.processPEs = processPEs;
	}

	public void setProcessAdapters(boolean processAdapters) {
		this.processAdapters = processAdapters;
	}

	@Override
	public void setApplicationContext(ApplicationContext ctx) throws BeansException {
		this.ctx = ctx;
	}

	public void setLatinFile(List<String> file) {
		this.latinFileLocation = file;
	}



	public void init() {

		try {

			if (latinFileLocation == null)
				throw new RuntimeException("LatinFile location cannot be null");

			for (String location : latinFileLocation) {
				Resource lfResource =  ctx.getResource(location);
				if (lfResource == null)
					throw new RuntimeException("LatinFile resource cannot be null");

				BufferedReader br;
				try {
					FileSystemManager fileSystemManager = VFS.getManager();
					FileObject fileObject = fileSystemManager.resolveFile(location);

					Reader reader = new InputStreamReader(fileObject.getContent().getInputStream());
					br = new BufferedReader(reader);

				} catch (Exception e) {
					// we tried using VFS, lets see if we can get it from the classloader

					File latinFile = lfResource.getFile();
					FileReader fi = new FileReader(latinFile);
					br =  new BufferedReader(fi);
				}
				
				
				String chunk ="",filecontent ="";
				while ((chunk = br.readLine()) != null) {
					filecontent += chunk + "\n";
				}
				
				if (processPEs) {
					System.out.println("Parse queries");
					DefaultListableBeanFactory moduleBeans = S4LatinBeansFactory.createPEs(ctx, filecontent);
					System.out.println("######## MODULE LOADED");
					for (String bn : moduleBeans.getBeanDefinitionNames()) {
						((FileSystemXmlApplicationContext) ctx).getBeanFactory().registerSingleton(bn,moduleBeans.getBean(bn));
						System.out.println("Registering bean: " + bn);
					}
				}
				if (processAdapters) {
					System.out.println("Parse adapters");
					DefaultListableBeanFactory adapterBeans = S4LatinBeansFactory.createAdapters(ctx, filecontent);
					System.out.println("######## ADAPTERS LOADED");
					for (String bn : adapterBeans.getBeanDefinitionNames()) {
						((FileSystemXmlApplicationContext) ctx).getBeanFactory().registerSingleton(bn,adapterBeans.getBean(bn));
						System.out.println("Registering bean: " + bn);
					}
					Map listenerBeanMap = ((FileSystemXmlApplicationContext) ctx).getBeansOfType(EventProducer.class);
					System.out.println("Bean size:" + listenerBeanMap.size());
				}

			}

		}

		catch (Exception e) {
			e.printStackTrace(System.err);
		}

	}


}
