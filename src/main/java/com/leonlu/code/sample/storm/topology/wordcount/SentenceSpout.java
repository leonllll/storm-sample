package com.leonlu.code.sample.storm.topology.wordcount;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class SentenceSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private String[] sentences = {
			"my dog has fleas",
			"i like cold beverages",
			"the dog ate my homework",
			"don't have a cow man",
			"i don't think i like fleas"
	};
	private int index = 0;
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}
	@Override
	public void open(Map config, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
	}
	@Override
	public void nextTuple() {
		this.collector.emit(new Values(sentences[index]));
		index++;
		if (index >= sentences.length) {
			index = 0;
		}
		Utils.sleep(2000);
	}
}
