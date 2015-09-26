
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class FileReaderSpout implements IRichSpout {
    public static final int TIME_BETWEEN_EMITS = 50;
    private SpoutOutputCollector _collector;
    private TopologyContext context;

    final private String inputFileName;

    private BufferedReader inputReader;

    public FileReaderSpout(String inputFileName) {
        this.inputFileName = inputFileName;
    }


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

     /*
    ----------------------TODO-----------------------
    Task: initialize the file reader
    ------------------------------------------------- */
        FileReader fileReader = null;
        try {
            fileReader = new FileReader(inputFileName);
        } catch (FileNotFoundException e) {
            handleCriticalException("Could not open input file", e);
        }

        inputReader = new BufferedReader(fileReader);


        this.context = context;
        this._collector = collector;
    }

    @Override
    public void nextTuple() {

     /*
    ----------------------TODO-----------------------
    Task:
    1. read the next line and emit a tuple for it
    2. don't forget to sleep when the file is entirely read to prevent a busy-loop

    ------------------------------------------------- */
        Utils.sleep(TIME_BETWEEN_EMITS);
        try {
            String line = inputReader.readLine();
            if(null != line) {
                _collector.emit(new Values(line));
            }
        } catch (IOException e) {
            handleCriticalException("Error reading file", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("word"));

    }

    @Override
    public void close() {
       /*
        ----------------------TODO-----------------------
        Task: close the file
        ------------------------------------------------- */

        try {
            inputReader.close();
        } catch (IOException e) {
            handleCriticalException("Could not close file", e);
        }

    }

    @Override
    public void activate() {
    }


    @Override
    public void deactivate() {
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private void handleCriticalException(String errorMessage, Exception e) {
        throw new RuntimeException(errorMessage, e);
    }
}
