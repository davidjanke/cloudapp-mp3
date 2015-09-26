import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * This topology reads a file and counts the words in that file
 */
public class TopWordFinderTopologyPartB {

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
        FileReaderSpout -> "spout"
        SplitSentenceBolt -> "split"
        WordCountBolt -> "count"
        ------------------------------------------------- */

        config.put("inputFileName", args[0]);

        IRichSpout spout = new FileReaderSpout(args[0]);
        SplitSentenceBolt splitSentenceBolt = new SplitSentenceBolt();
        WordCountBolt wordCountBolt = new WordCountBolt();

        builder.setSpout(spoutName, spout, 1);
        builder.setBolt(splitBoltName, splitSentenceBolt, 8).shuffleGrouping(spoutName);
        builder.setBolt("count", wordCountBolt).fieldsGrouping(splitBoltName, new Fields("word"));

        config.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count", config, builder.createTopology());

        //wait for 2 minutes and then kill the job
        Thread.sleep( 2 * 60 * 1000);

        cluster.shutdown();
    }
}
