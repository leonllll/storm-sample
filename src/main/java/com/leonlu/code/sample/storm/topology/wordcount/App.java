package com.leonlu.code.sample.storm.topology.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.utils.Utils;

public class App 
{	
    public static void main( String[] args ) {
    	Config config = new Config();
    	config.setNumWorkers(3);
		try {
			StormSubmitter.submitTopologyWithProgressBar("storm-tester", config, WordCountTopology.buildTopology());
		} catch (Exception e) {
			e.printStackTrace();
		} 
    }
}
