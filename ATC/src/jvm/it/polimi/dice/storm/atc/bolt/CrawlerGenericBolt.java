package it.polimi.dice.storm.atc.bolt;

import java.util.Map;
import java.util.Random;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * A bolt that counts the words that it receives
 */
public class CrawlerGenericBolt extends BaseRichBolt
{
  // To output tuples from this bolt to the next stage bolts, if any
  private OutputCollector collector;

  private int alpha;
  private double sigma;
  String componentId;

  public CrawlerGenericBolt(int alpha, double sigma) {
    this.alpha = alpha;
    this.sigma = sigma;
}
  
  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {

    // save the collector for emitting tuples
    collector = outputCollector;
    componentId = topologyContext.getThisComponentId();
    
  }

  @Override
  public void execute(Tuple tuple)
  {
    // get the word from the 1st column of incoming tuple
    String word = tuple.getString(0);
    StringBuilder decoratedWord = new StringBuilder();
    decoratedWord.append(word).append("->" + componentId);
    
    Utils.sleep(this.alpha);
    Random generator = new Random();
    double number = generator.nextDouble();
    if (number < sigma){
        // emit the word and count
        collector.emit(new Values(decoratedWord.toString()));        
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
  {
    // tell storm the schema of the output tuple for this spout
    // tuple consists of a two columns called 'word' and 'count'

    // declare the first column 'word', second column 'count'
    outputFieldsDeclarer.declare(new Fields("word"));
  }
}
