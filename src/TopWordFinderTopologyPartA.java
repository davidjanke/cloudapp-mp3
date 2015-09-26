
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * This topology counts the words from sentences emmited from a random sentence spout.
 */
public class TopWordFinderTopologyPartA {

    static String spoutName = "spout";
    static String splitBoltName = "split";

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        Config config = new Config();
        config.setDebug(true);


        /*
        ----------------------TODO-----------------------
        Task: wire up the topology

        NOTE:make sure when connecting components together, using the functions setBolt(name,…) and setSpout(name,…),
        you use the following names for each component:

        RandomSentanceSpout -> "spout"
        SplitSentenceBolt -> "split"
        WordCountBolt -> "count"
        ------------------------------------------------- */

        RandomSentenceSpout randomSentenceSpout = new RandomSentenceSpout();
        SplitSentenceBolt splitSentenceBolt = new SplitSentenceBolt();
        WordCountBolt wordCountBolt = new WordCountBolt();

        builder.setSpout(spoutName, randomSentenceSpout, 5);
        builder.setBolt(splitBoltName, splitSentenceBolt, 8).shuffleGrouping(spoutName);
        builder.setBolt("count", wordCountBolt).fieldsGrouping(splitBoltName, new Fields("word"));



        config.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count", config, builder.createTopology());

        //wait for 60 seconds and then kill the topology
        Thread.sleep(60 * 1000);

        cluster.shutdown();
    }
}
