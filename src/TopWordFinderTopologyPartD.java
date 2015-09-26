
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.*;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * This topology reads a file and counts the words in that file, then finds the top N words.
 */
public class TopWordFinderTopologyPartD {

    private static final int N = 10;

    static final String countBoltName = "count";
    static final String spoutName = "spout";
    static final String splitBoltName = "split";
    static final String normalizeBoltName = "normalize";
    static final String topNBoltName = "top-n";

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
        NormalizerBolt -> "normalize"
        TopNFinderBolt -> "top-n"

        ------------------------------------------------- */

        config.put("inputFileName", args[0]);

        IRichSpout spout = new FileReaderSpout(args[0]);
        IBasicBolt splitSentenceBolt = new SplitSentenceBolt();
        IBasicBolt wordCountBolt = new WordCountBolt();
        IBasicBolt normalizerBolt = new NormalizerBolt();
        IBasicBolt topNFinderBolt = new TopNFinderBolt(N);

        builder.setSpout(spoutName, spout, 1);
        builder.setBolt(splitBoltName, splitSentenceBolt, 8).shuffleGrouping(spoutName);
        builder.setBolt(normalizeBoltName, normalizerBolt, 8).shuffleGrouping(splitBoltName);
        builder.setBolt(countBoltName, wordCountBolt).fieldsGrouping(normalizeBoltName, new Fields("word"));
        builder.setBolt(topNBoltName, topNFinderBolt, 1).allGrouping(countBoltName);

        config.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count", config, builder.createTopology());

        //wait for 2 minutes and then kill the job
        Thread.sleep(2 * 60 * 1000);

        cluster.shutdown();
    }
}
