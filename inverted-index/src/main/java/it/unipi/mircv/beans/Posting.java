package it.unipi.mircv.beans;

public class Posting {
    long docId;
    Integer frequency;

    public Posting(long doc_id, Integer frequency) {
        this.docId = doc_id;
        this.frequency = frequency;
    }

    public long getDocId() {
        return docId;
    }

    public Integer getFrequency() {
        return frequency;
    }

    public void incrementFrequency() {
        frequency++;
    }
    @Override
    public String toString() {
        return "[" + docId +
                ", " + frequency +
                ']';
    }
}

