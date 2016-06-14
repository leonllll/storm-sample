package com.leonlu.code.sample.storm.util;

import static org.junit.Assert.*;

import java.util.Arrays;
import org.junit.Before;
import org.junit.Test;
import com.leonlu.code.sample.storm.topology.wordcount.WordCountTopology;
import com.leonlu.code.sample.storm.util.RemoteSubmitter;
import com.leonlu.code.sample.storm.util.RemoteSubmitter.ReflectedTopologyOption;

import backtype.storm.Config;

public class RemoteSubmitterTest {
	private Config config;
	
	@Before
	public void setUp(){
		config = new Config();
		config.put(Config.NIMBUS_HOST,"9.119.84.179");   
		config.put(Config.NIMBUS_THRIFT_PORT, 6627);
		config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("9.119.84.177","9.119.84.179","9.119.84.250")); 
		config.put(Config.STORM_ZOOKEEPER_PORT,2181);
		
		config.put(Config.TOPOLOGY_WORKERS, 3);
	}
	@Test
	public void testSubmitTopologySubmitLocalTopologyWay1() {	
		
		RemoteSubmitter.submitLocalTopologyWay1("test-1", config, 
				WordCountTopology.buildTopology(), 
				"C:\\MyWorkspace\\project\\my-project\\storm-sample\\target\\"
						+ "storm-sample-0.0.1-SNAPSHOT-jar-with-dependencies.jar");
	}

	@Test
	public void testSubmitTopologySubmitLocalTopologyWay2() {		
		RemoteSubmitter.submitLocalTopologyWay2("test-2", config, 
				WordCountTopology.buildTopology(), 
				"C:\\MyWorkspace\\project\\my-project\\storm-sample\\target\\"
						+ "storm-sample-0.0.1-SNAPSHOT-jar-with-dependencies.jar");
	}
	
	@Test
	public void testSubmitTopologyMapStringConfigReflectedTopologyOption() {
		
		ReflectedTopologyOption reflectedTopologyOption = new ReflectedTopologyOption(
				"C:\\MyWorkspace\\project\\bigdata\\bdp-storm-tester\\target\\"
				+ "bdp-storm-tester-0.0.1-SNAPSHOT-jar-with-dependencies.jar", 
				"com.ibm.gbsc.bdp.storm.tester.WordCountTopology", 
				"buildTopology",  
				new Class[] {},
				new Object[] {});
		RemoteSubmitter.submitReflectedTopology("test-3", config, reflectedTopologyOption);
		
	}

}
