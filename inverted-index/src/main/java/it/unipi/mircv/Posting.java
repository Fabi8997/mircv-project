package it.unipi.mircv;

public class Posting {
    Integer doc_id;
    Integer frequency;

    public Posting(Integer doc_id, Integer frequency) {
        this.doc_id = doc_id;
        this.frequency = frequency;
    }

    public Integer getDoc_id() {
        return doc_id;
    }

    public Integer getFrequency() {
        return frequency;
    }
}

