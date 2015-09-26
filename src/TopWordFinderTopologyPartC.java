import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * This topology reads a file, splits the senteces into words, normalizes the words such that all words are
 * lower case and common words are removed, and then count the number of words.
 */
public class TopWordFinderTopologyPartC {

    public static final String countBoltName = "count";
    static String spoutName = "spout";
    static String splitBoltName = "split";
    static String normalizeBoltName = "normalize";


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

        ------------------------------------------------- */

        config.put("inputFileName", args[0]);

        IRichSpout spout = new FileReaderSpout(args[0]);
        IBasicBolt splitSentenceBolt = new SplitSentenceBolt();
        IBasicBolt wordCountBolt = new WordCountBolt();
        IBasicBolt normalizerBolt = new NormalizerBolt();

        builder.setSpout(spoutName, spout, 1);
        builder.setBolt(splitBoltName, splitSentenceBolt, 8).shuffleGrouping(spoutName);
        builder.setBolt(normalizeBoltName, normalizerBolt, 8).shuffleGrouping(splitBoltName);
        builder.setBolt(countBoltName, wordCountBolt).fieldsGrouping(normalizeBoltName, new Fields("word"));

        config.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count", config, builder.createTopology());

        //wait for 2 minutes then kill the job
        Thread.sleep(2 * 60 * 1000);

        cluster.shutdown();
    }
}
