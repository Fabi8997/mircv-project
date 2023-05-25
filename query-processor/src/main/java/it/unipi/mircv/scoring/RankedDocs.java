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

    /**
     * @param longDoubleTuple element to be added to the list
     * @return the boolean result of the add operation
     */
    @Override
    public boolean add(Tuple<Long, Double> longDoubleTuple) {
        boolean result = super.add(longDoubleTuple);

        //if the list has at least K elements, the threshold must be updated
        if(result && this.size() >= K){
            Iterator<Tuple<Long, Double>> iterator = iterator();
            int counter = 0;
            //traverse, in descending order, the list until the K-1th element
            while(iterator.hasNext()){
                iterator.next();
                counter++;
                if(counter == K - 1){
                    break;
                }
            }

            //updates the threshold with the value of the Kth element
            this.threshold = iterator.next().getSecond();

        }
        return result;
    }

    /**
     * @return return the current threshold
     */
    public double getThreshold() {
        return threshold;
    }
}
