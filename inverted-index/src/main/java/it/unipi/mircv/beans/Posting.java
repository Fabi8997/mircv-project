package it.unipi.mircv.beans;

public class Posting {
    long doc_id;
    Integer frequency;

    public Posting(long doc_id, Integer frequency) {
        this.doc_id = doc_id;
        this.frequency = frequency;
    }

    public long getDoc_id() {
        return doc_id;
    }

    public Integer getFrequency() {
        return frequency;
    }

    public void incrementFrequency() {
        frequency++;
    }
    @Override
    public String toString() {
        return "Posting{" +
                "doc_id=" + doc_id +
                ", frequency=" + frequency +
                '}';
    }
}

