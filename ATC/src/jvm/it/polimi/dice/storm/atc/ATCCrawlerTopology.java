package it.polimi.dice.storm.atc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import it.polimi.dice.storm.atc.bolt.CrawlerGenericBolt;
import it.polimi.dice.storm.atc.spout.CrawlerGenericSpout;

/**
 * This is a basic example of a Storm topology.
 */

/**
 * This is a basic example of a storm topology.
 *
 * This topology demonstrates how to add three exclamation marks '!!!' to each
 * word emitted
 *
 * This is an example for Udacity Real Time Analytics Course - ud381
 *
 */
public class ATCCrawlerTopology {

    public static void main(String[] args) throws Exception {
        // create the topology
        TopologyBuilder builder = new TopologyBuilder();

        // attach the word spout to the topology - parallelism of 10
        builder.setSpout("redisSpout", new CrawlerGenericSpout(1), 5);

        // attach the exclamation bolt to the topology - parallelism of 3
        builder.setBolt("deserializationBolt", new CrawlerGenericBolt(1, 0.9), 5).shuffleGrouping("redisSpout");

        // attach another exclamation bolt to the topology - parallelism of 2
        builder.setBolt("urlExpansionBolt", new CrawlerGenericBolt(1, 0.9), 4).shuffleGrouping("deserializationBolt");

        builder.setBolt("urlCrawlDeciderBolt", new CrawlerGenericBolt(1, 0.9), 1).shuffleGrouping("urlExpansionBolt");

        builder.setBolt("webPageFetcherBolt", new CrawlerGenericBolt(1, 0.8), 4).shuffleGrouping("urlCrawlDeciderBolt");

        builder.setBolt("mediaExtractionBolt", new CrawlerGenericBolt(1, 1), 1).shuffleGrouping("urlCrawlDeciderBolt");

        builder.setBolt("articleExtractionBolt", new CrawlerGenericBolt(1, 0.8), 8)
                .shuffleGrouping("webPageFetcherBolt");

        builder.setBolt("solrBolt", new CrawlerGenericBolt(1, 1), 1)
                .shuffleGrouping("mediaExtractionBolt")
                .shuffleGrouping("articleExtractionBolt");

        // create the default config object
        Config conf = new Config();

        // set the config in debugging mode
        conf.setDebug(true);

        if (args != null && args.length > 0) {

            // run it in a live cluster

            // set the number of workers for running all spout and bolt tasks
            conf.setNumWorkers(3);

            // create the topology and submit with config
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

        } else {

            // run it in a simulated local cluster

            // create the local cluster instance
            LocalCluster cluster = new LocalCluster();

            // submit the topology to the local cluster
            cluster.submitTopology("atc", conf, builder.createTopology());

            // let the topology run for 60 seconds. note topologies never
            // terminate!
            Thread.sleep(7200000);

            // kill the topology
            cluster.killTopology("atc");

            // we are done, so shutdown the local cluster
            cluster.shutdown();
        }
    }
}
