package it.unipi.mircv.beans;

import it.unipi.mircv.utils.Utils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class TermInfo {

    private String term;
    private long offsetDocId;
    private long offsetFrequency;
    private double idf;
    private int docIdsBytesLength;

    private int frequenciesBytesLength;
    private int postingListLength;

    //Length in bytes of the term field
    public final static int TERM_LENGTH = 48;

    //Length in bytes of the offsetDocId field
    public final static int OFFSET_DOCIDS_LENGTH = 8;

    //Length in bytes of the frequency length field
    public final static int OFFSET_FREQUENCIES_LENGTH = 8;

    public final static int BYTES_DOCID_LENGTH = 4;

    public final static int BYTES_FREQUENCY_LENGTH = 4;

    //Length in bytes of the postingListLength field
    public final static int POSTING_LIST_LENGTH = 4;
    public final static int IDF_LENGTH = 8;

    public final static int TERM_INFO_LENGTH = TERM_LENGTH + OFFSET_DOCIDS_LENGTH + OFFSET_FREQUENCIES_LENGTH + BYTES_DOCID_LENGTH + BYTES_FREQUENCY_LENGTH + POSTING_LIST_LENGTH + IDF_LENGTH;

    public TermInfo(String term, long offsetDocId, long offsetFrequency, double idf, int docIdsBytesLength, int frequenciesBytesLength, int postingListLength) {
        this.term = term;
        this.offsetDocId = offsetDocId;
        this.offsetFrequency = offsetFrequency;
        this.idf = idf;
        this.docIdsBytesLength = docIdsBytesLength;
        this.frequenciesBytesLength = frequenciesBytesLength;
        this.postingListLength = postingListLength;
    }

    public TermInfo(String term, long offsetDocId, long offsetFrequency, int bytesDocId, int bytesFrequency, int postingListLength) {
        this.term = term;
        this.offsetDocId = offsetDocId;
        this.offsetFrequency = offsetFrequency;
        this.docIdsBytesLength = bytesDocId;
        this.frequenciesBytesLength = bytesFrequency;
        this.postingListLength = postingListLength;
    }

    public TermInfo(String term, long offsetDocId, long offsetFrequency, int postingListLength) {
        this.term = term;
        this.offsetDocId = offsetDocId;
        this.offsetFrequency = offsetFrequency;
        this.postingListLength = postingListLength;
    }

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

    public String getTerm() {
        return term;
    }

    public long getOffsetDocId() {
        return offsetDocId;
    }

    public void setOffsetDocId(int offsetDocId) {
        this.offsetDocId = offsetDocId;
    }

    public long getOffsetFrequency() {
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

    public int getDocIdsBytesLength() {
        return docIdsBytesLength;
    }

    public double getIdf() {
        return idf;
    }

    public int getFrequenciesBytesLength() {
        return frequenciesBytesLength;
    }

    public void set(int offsetDocId, int offsetFrequency, int postingListLength){
        this.setOffsetDocId(offsetDocId);
        this.setOffsetFrequency(offsetFrequency);
        this.setPostingListLength(postingListLength);
    }

    /**
     * Write the term info to a file. This method is used during the building of the partial blocks.
     * @param lexiconFile Is the random access file on which the term info is written.
     * @param key Term to be written.
     * @param termInfo Information of the term to be written.
     */
    public void writeToFile(RandomAccessFile lexiconFile, String key, TermInfo termInfo){

        //Fill with whitespaces to keep the length standard
        String tmp = Utils.leftpad(key, TERM_LENGTH);

        byte[] term = ByteBuffer.allocate(TERM_LENGTH).put(tmp.getBytes()).array();
        byte[] offsetDocId = ByteBuffer.allocate(OFFSET_DOCIDS_LENGTH).putLong(termInfo.getOffsetDocId()).array();
        byte[] offsetFrequency = ByteBuffer.allocate(OFFSET_FREQUENCIES_LENGTH).putLong(termInfo.getOffsetFrequency()).array();
        byte[] postingListLength = ByteBuffer.allocate(POSTING_LIST_LENGTH).putInt(termInfo.getPostingListLength()).array();

        try {
            lexiconFile.write(term);
            lexiconFile.write(offsetDocId);
            lexiconFile.write(offsetFrequency);
            lexiconFile.write(postingListLength);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Write the term info to a file. This method is used during the merge of the partial blocks, here we have
     * all the information directly inside the termInfo object.
     * @param lexiconFile Is the random access file on which the term info is written.
     * @param termInfo Information of the term to be written.
     */
    public void writeToFile(RandomAccessFile lexiconFile, TermInfo termInfo){

        // TODO: 30/03/2023 Add also the TFIDF and BM25 writes

        //Fill with whitespaces to keep the length standard
        String tmp = Utils.leftpad(termInfo.getTerm(), TERM_LENGTH);

        byte[] term = ByteBuffer.allocate(TERM_LENGTH).put(tmp.getBytes()).array();
        byte[] offsetDocId = ByteBuffer.allocate(OFFSET_DOCIDS_LENGTH).putLong(termInfo.getOffsetDocId()).array();
        byte[] offsetFrequency = ByteBuffer.allocate(OFFSET_FREQUENCIES_LENGTH).putLong(termInfo.getOffsetFrequency()).array();
        byte[] bytesDocId = ByteBuffer.allocate(BYTES_DOCID_LENGTH).putInt(termInfo.getDocIdsBytesLength()).array();
        byte[] bytesFrequency = ByteBuffer.allocate(BYTES_FREQUENCY_LENGTH).putInt(termInfo.getFrequenciesBytesLength()).array();
        byte[] postingListLength = ByteBuffer.allocate(POSTING_LIST_LENGTH).putInt(termInfo.getPostingListLength()).array();
        byte[] idf = ByteBuffer.allocate(IDF_LENGTH).putDouble(termInfo.getIdf()).array();
        try {
            lexiconFile.write(term);
            lexiconFile.write(offsetDocId);
            lexiconFile.write(offsetFrequency);
            lexiconFile.write(idf);
            lexiconFile.write(bytesDocId);
            lexiconFile.write(bytesFrequency);
            lexiconFile.write(postingListLength);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "TermInfo{" +
                "term='" + term + '\'' +
                ", offsetDocId=" + offsetDocId +
                ", offsetFrequency=" + offsetFrequency +
                ", docIdsBytesLength=" + docIdsBytesLength +
                ", frequenciesBytesLength=" + frequenciesBytesLength +
                ", postingListLength=" + postingListLength +
                ", idf=" + idf +
                '}';
    }
}
