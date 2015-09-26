import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.collect.ComparisonChain;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * a bolt that finds the top n words.
 */
public class TopNFinderBolt extends BaseBasicBolt {
    private final HashMap<String, Integer> currentTopWords;

    private final long intervalToReport = 20;
    private long lastReportTime = System.currentTimeMillis();

    public TopNFinderBolt(int N) {
        currentTopWords = new TopNWordCountMap(N);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        /*
        ----------------------TODO-----------------------
        Task: keep track of the top N words

        ------------------------------------------------- */

        currentTopWords.put(tuple.getString(0), tuple.getInteger(1));

        //reports the top N words periodically
        if (System.currentTimeMillis() - lastReportTime >= intervalToReport) {
            collector.emit(new Values(printMap()));
            lastReportTime = System.currentTimeMillis();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("top-N"));
    }

    private static class TopNWordCountMap extends HashMap<String, Integer> {
        private final int limit;
        private final TopXTreeSet<WordCountPair> values;

        public TopNWordCountMap(int entryLimit) {
            super(entryLimit * 2);
            this.limit = entryLimit;
            values = new TopXTreeSet<WordCountPair>(entryLimit);
        }

        public Integer put(String key, Integer value) {
            WordCountPair pair = WordCountPair.of(key, value);
            if(checkIfToBeAdded(pair)) {
                Integer oldValue = super.put(key, value);
                maintain();
                return oldValue;
            } else {
                return null;
            }
        }

        private void maintain() {
            values.clear();
            for(Map.Entry<String, Integer> entry:entrySet()) {
                values.add(WordCountPair.of(entry.getKey(), entry.getValue()));
            }
            clear();
            for(WordCountPair pair: values) {
                super.put(pair.getWord(), pair.getCount());
            }
        }

        private boolean checkIfToBeAdded(WordCountPair pair) {
            if (size() < limit) {
                return true;
            } else {
                return greaterThanSmallestValue(pair);
            }
        }

        private boolean greaterThanSmallestValue(WordCountPair pair) {
            return values.first().compareTo(pair) < 0;
        }


    }

    private static class TopXTreeSet<E> extends TreeSet<E> {

        private final int limit;

        public TopXTreeSet(int limit) {
            this.limit = limit;
        }

        @Override
        public boolean add(E e) {
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

    private static class WordCountPair extends Pair<Integer, String> {

        private WordCountPair(String word, Integer count) {
            super(count, word);
        }

        public static WordCountPair of(String word, Integer count) {
            return new WordCountPair(word, count);
        }

        public Integer getCount() {
            return first;
        }

        public String getWord() {
            return second;
        }

    }

    private static class Pair<A extends Comparable<? super A>,
            B extends Comparable<? super B>>
            implements Comparable<Pair<A, B>> {

        public final A first;
        public final B second;

        private Pair(A first, B second) {
            this.first = first;
            this.second = second;
        }

        public static <A extends Comparable<? super A>,
                B extends Comparable<? super B>>
        Pair<A, B> of(A first, B second) {
            return new Pair<A, B>(first, second);
        }

        @Override
        public int compareTo(Pair<A, B> o) {
            return ComparisonChain.start().compare(first, o.first).compare(second, o.second).result();
        }

        @Override
        public int hashCode() {
            return 31 * hashcode(first) + hashcode(second);
        }

        private static int hashcode(Object o) {
            return o == null ? 0 : o.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Pair))
                return false;
            if (this == obj)
                return true;
            return equal(first, ((Pair<?, ?>) obj).first)
                    && equal(second, ((Pair<?, ?>) obj).second);
        }

        private boolean equal(Object o1, Object o2) {
            return o1 == o2 || (o1 != null && o1.equals(o2));
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
