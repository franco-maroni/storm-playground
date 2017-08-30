package it.polimi.dice.storm.atc.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public class CrawlerGenericSpout extends BaseRichSpout {
  SpoutOutputCollector _collector;
  Random _rand;
  double avgEmitRate;
  int waitTimeToEmit;

  public CrawlerGenericSpout(double avgEmitRate) {
      this.avgEmitRate = avgEmitRate;
      this.waitTimeToEmit = (int)(1/avgEmitRate + 0.5d);
}

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    _rand = new Random();
  }

  @Override
  public void nextTuple() { 
    Utils.sleep(waitTimeToEmit);
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
