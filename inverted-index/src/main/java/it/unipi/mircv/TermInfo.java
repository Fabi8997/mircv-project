package it.unipi.mircv;

public class TermInfo {

    private int termId;
    private int offset;


    public TermInfo(int termId, int offset) {
        this.termId = termId;
        this.offset = offset;
    }

    public int getTermId() {
        return termId;
    }

    public int getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return "TermInfo{" +
                "termId=" + termId +
                ", offset=" + offset +
                '}';
    }
}
