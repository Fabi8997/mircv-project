package it.unipi.mircv.beans;

public class Tuple<K, V> {

    private final K first;
    private final V second;

    public Tuple(K first, V second) {
        this.first = first;
        this.second = second;
    }

    public K getFirst() {
        return first;
    }

    public V getSecond() {
        return second;
    }

    @Override
    public String toString() {
        return "("+ first +
                ", " + second +
                ')';
    }
}