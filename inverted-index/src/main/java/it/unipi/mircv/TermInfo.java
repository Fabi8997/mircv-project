package it.unipi.mircv;

public class TermInfo {

    private int offsetDocId;
    private int offsetFrequency;
    private int postingListLength;


    public TermInfo(int offsetDocId, int offsetFrequency, int postingListLength) {
        this.offsetDocId = offsetDocId;
        this.offsetFrequency = offsetFrequency;
        this.postingListLength = postingListLength;
    }

    public TermInfo() {
        this.offsetDocId = 0;
        this.offsetFrequency = 0;
        this.postingListLength = 0;
    }

    public int getOffsetDocId() {
        return offsetDocId;
    }

    public void setOffsetDocId(int offsetDocId) {
        this.offsetDocId = offsetDocId;
    }

    public int getOffsetFrequency() {
        return offsetFrequency;
    }

    public void setOffsetFrequency(int offsetFrequency) {
        this.offsetFrequency = offsetFrequency;
    }

    public int getPostingListLength() {
        return postingListLength;
    }

    public void setPostingListLength(int postingListLength) {
        this.postingListLength = postingListLength;
    }

    public void set(int offsetDocId, int offsetFrequency, int postingListLength){
        this.setOffsetDocId(offsetDocId);
        this.setOffsetFrequency(offsetFrequency);
        this.setPostingListLength(postingListLength);
    }

    @Override
    public String toString() {
        return "TermInfo{" +
                "offsetDocId=" + offsetDocId +
                ", offsetFrequency=" + offsetFrequency +
                ", postingListLength=" + postingListLength +
                '}';
    }
}
