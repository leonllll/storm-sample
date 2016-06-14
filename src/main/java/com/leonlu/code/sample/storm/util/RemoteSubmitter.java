package com.leonlu.code.sample.storm.util;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import org.json.simple.JSONValue;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

public class RemoteSubmitter { 
	
	public static class ReflectedTopologyOption {
		public String localJar;
		public String className;
		public String methodName;
		public Class[] methodArgsClasses;
		public Object[] methodArgs;
		
		public ReflectedTopologyOption() {
			
		}
		public ReflectedTopologyOption(String localJar, String className,
				String methodName, Class[] methodArgsClasses,
				Object[] methodArgs) {
			this.localJar = localJar;
			this.className = className;
			this.methodName = methodName;
			this.methodArgsClasses = methodArgsClasses;
			this.methodArgs = methodArgs;
		}
		
	}
	public static void submitLocalTopologyWay1(String topologyName, Config topologyConf, 
			StormTopology topology, String localJar) {
		try {
			//get default storm config
			Map defaultStormConf = Utils.readStormConfig();
			defaultStormConf.putAll(topologyConf);
			
			//set JAR
			System.setProperty("storm.jar",localJar);
			
			//submit topology
			StormSubmitter.submitTopology(topologyName, defaultStormConf, topology);
			
		} catch (Exception e) {
			String errorMsg = "can't deploy topology " + topologyName + ", " + e.getMessage();
			System.out.println(errorMsg);
			e.printStackTrace();
		} 
	}
	public static void submitLocalTopologyWay2(String topologyName, Config topologyConf, 
			StormTopology topology, String localJar) {
		try {
			//get default storm config
			Map defaultStormConf = Utils.readStormConfig();
			defaultStormConf.putAll(topologyConf);	
			
			//upload JAR
			String remoteJar = StormSubmitter.submitJar(defaultStormConf, localJar);
			
			//get nimbus client and submit topology
			Client client = NimbusClient.getConfiguredClient(defaultStormConf).getClient();
			client.submitTopology(topologyName, remoteJar, JSONValue.toJSONString(topologyConf), topology);
			
		} catch (Exception e) {
			String errorMsg = "can't deploy topology " + topologyName + ", " + e.getMessage();
			System.out.println(errorMsg);
			e.printStackTrace();
		} 
	}
	public static void submitReflectedTopology(String topologyName, Config topologyConf,  
			ReflectedTopologyOption reflectedTopologyOption) {
		try {
			//get reflected topology
			StormTopology topology = getReflectedTopology(reflectedTopologyOption);
			
			//submit reflected topology
			submitLocalTopologyWay1(topologyName, topologyConf, topology, reflectedTopologyOption.localJar);
			
		} catch (Exception e) {
			String errorMsg = "can't deploy topology " + topologyName + ", " + e.getMessage();
			System.out.println(errorMsg);
			e.printStackTrace();
		} 
	}
	
	private static StormTopology getReflectedTopology(ReflectedTopologyOption topologyOption) 
			throws Exception{
		URLClassLoader classLoader = (URLClassLoader)ClassLoader.getSystemClassLoader();
	    Method addUrl = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
	    addUrl.setAccessible(true);
	    URL url = new File(topologyOption.localJar).toURI().toURL();
	    addUrl.invoke(classLoader, url);
	    
	    Class<?> clazz = classLoader.loadClass(topologyOption.className);
	    Object instance = clazz.newInstance ();
	    Method method = clazz.getMethod(topologyOption.methodName, topologyOption.methodArgsClasses); 
	    return (StormTopology) method.invoke(instance, topologyOption.methodArgs);
	}
	
	
}
