package it.unipi.mircv.scoring;

import it.unipi.mircv.beans.Tuple;

import java.util.Iterator;
import java.util.PriorityQueue;

public class RankedDocs extends PriorityQueue<Tuple<Long, Double>> {
    private double threshold;
    private final int K;
    public RankedDocs(int K){
        super((o1, o2) -> o2.getSecond().compareTo(o1.getSecond()));
        this.threshold = 0;
        this.K = K;
    }

    @Override
    public boolean add(Tuple<Long, Double> longDoubleTuple) {
        boolean result = super.add(longDoubleTuple);
        if(result && this.size() >= K){
            Iterator<Tuple<Long, Double>> iterator = iterator();
            int counter = 0;
            while(iterator.hasNext()){
                iterator.next();
                counter++;
                if(counter == K - 1){
                    break;
                }
            }

            this.threshold = iterator.next().getSecond();

        }
        return result;
    }

    public double getThreshold() {
        return threshold;
    }
}