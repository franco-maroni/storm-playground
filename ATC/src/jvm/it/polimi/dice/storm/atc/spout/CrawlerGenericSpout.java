package it.polimi.dice.storm.atc.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public class CrawlerGenericSpout extends BaseRichSpout {
  SpoutOutputCollector _collector;
  Random _rand;
  double avgEmitRate;
  int waitTimeToEmit;

  public CrawlerGenericSpout(double avgEmitRate) {
      this.avgEmitRate = avgEmitRate;
      this.waitTimeToEmit = (int)(1/avgEmitRate * 1000 + 0.5d);
}

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    _rand = new Random();
  }

  @Override
  public void nextTuple() { 
    Utils.sleep(100);
    String[] sentences = new String[]{
      "A","B","C","D","E"
      };
    String sentence = sentences[_rand.nextInt(sentences.length)];
    _collector.emit(new Values(sentence));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("sentence"));
  }

}
