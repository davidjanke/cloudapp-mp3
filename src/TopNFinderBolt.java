import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * a bolt that finds the top n words.
 */
public class TopNFinderBolt extends BaseBasicBolt {
    private HashMap<String, Integer> currentTopWords = new HashMap<String, Integer>();

    private int N;

    private final long intervalToReport = 20;
    private long lastReportTime = System.currentTimeMillis();

    public TopNFinderBolt(int N) {
        this.N = N;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        /*
        ----------------------TODO-----------------------
        Task: keep track of the top N words

        ------------------------------------------------- */

        currentTopWords.put(tuple.getString(0), tuple.getInteger(1));
        cleanToTopNWords();

        //reports the top N words periodically
        if (System.currentTimeMillis() - lastReportTime >= intervalToReport) {
            collector.emit(new Values(printMap()));
            lastReportTime = System.currentTimeMillis();
        }
    }

    private void cleanToTopNWords() {
        TreeSet<Integer> sortedCounts = new TopXTreeSet<Integer>(N);
        sortedCounts.addAll(currentTopWords.values());

        int minimum = sortedCounts.first();

        for(String word : currentTopWords.keySet()) {
            if(currentTopWords.get(word) < minimum)
            currentTopWords.remove(word);
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("top-N"));

    }

/*
    private static class TopNValueHashMap<K, V extends Comparable<V>> extends HashMap<K, V> {
        private final TopXTreeSet<V> valueTopSet;
        private final HashMap<K,V> backingHashMap = new HashMap<K, V>();
        private final int limit;

        public TopNValueHashMap(Integer entryLimit) {
            this.limit = entryLimit;
            valueTopSet = new TopXTreeSet<V>(entryLimit);
        }

        @Override
        public V put(K key, V value) {
            checkIfToBeAdded(value);
            return super.put(key, value);
        }

        private boolean checkIfToBeAdded(K key, V value) {
            if (valueTopSet.size() < limit) {
                return true;
            } else {
                return (valueTopSet.first().compareTo(value) < 0);
            }
        }


    }
*/
    private static class TopXTreeSet<V> extends TreeSet<V> {
        private final int limit;

        public TopXTreeSet(int limit) {
            this.limit = limit;
        }

        @Override
        public boolean add(V e) {
            boolean addSuccess = super.add(e);
            if(addSuccess) {
                reduceToLimit();
            }
            return addSuccess;
        }

        private void reduceToLimit() {
            if(size() > limit) {
                remove(first());
            }
        }
    }

    public String printMap() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("top-words = [ ");
        for (String word : currentTopWords.keySet()) {
            stringBuilder.append("(" + word + " , " + currentTopWords.get(word) + ") , ");
        }
        int lastCommaIndex = stringBuilder.lastIndexOf(",");
        stringBuilder.deleteCharAt(lastCommaIndex + 1);
        stringBuilder.deleteCharAt(lastCommaIndex);
        stringBuilder.append("]");
        return stringBuilder.toString();

    }

}
