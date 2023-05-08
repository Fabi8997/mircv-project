package it.unipi.mircv.beans;

import it.unipi.mircv.utils.Utils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * Class that holds the information about a skip block.
 */
public class SkipBlock {

    //starting offset of the respective doc id block in the docids.txt file.
    long startDocidOffset;

    //Length in number of postings if not compressed or in byte if compressed
    int skipBlockDocidLength;

    //starting offset of the respective freq block in the frequencies.txt file.
    long startFreqOffset;

    //Length in number of postings if not compressed or in byte if compressed
    int skipBlockFreqLength;

    //maximum doc id in the block represented by this skipBlock.
    long maxDocid;

    int OFFSET_LENGTH = 8;

    int SKIP_BLOCK_DIMENSION_LENGTH = 4;

    int MAX_DOC_ID_LENGTH = 8;

    //Length in byte of each skip block (32)
    int SKIP_BLOCK_LENGTH = 2*OFFSET_LENGTH + 2*SKIP_BLOCK_DIMENSION_LENGTH + MAX_DOC_ID_LENGTH;

    /**
     * Constructor for the SkipBlock class.
     * @param startDocidOffset starting offset of the respective doc id block in the docids.txt file.
     * @param startFreqOffset starting offset of the respective freq block in the frequencies.txt file.
     * @param maxDocid maximum doc id in the block represented by this skipBlock.
     */
    public SkipBlock(long startDocidOffset, long startFreqOffset, long maxDocid) {
        this.startDocidOffset = startDocidOffset;
        this.startFreqOffset = startFreqOffset;
        this.maxDocid = maxDocid;
    }

    /**
     * Constructor for the SkipBlock class.
     * @param startDocidOffset starting offset of the respective doc id block in the docids.txt file.
     * @param maxDocid maximum doc id in the block represented by this skipBlock.
     */
    public SkipBlock(long startDocidOffset, int skipBlockDocidLength, long maxDocid) {
        this.startDocidOffset = startDocidOffset;
        this.skipBlockDocidLength = skipBlockDocidLength;
        this.skipBlockFreqLength = -1;
        this.startFreqOffset = -1;
        this.maxDocid = maxDocid;
    }

    public void setFreqInfo(long startFreqOffset, int skipBlockFreqLength) {
        this.startFreqOffset = startFreqOffset;
        this.skipBlockFreqLength = skipBlockFreqLength;
    }

    public void setSkipBlockDocidLength(int skipBlockDocidLength) {
        this.skipBlockDocidLength = skipBlockDocidLength;
    }

    /**
     * Write the term info to a file. This method is used during the merge of the partial blocks, here we have
     * all the information directly inside the termInfo object.
     * @param skipBlocksFile Is the random access file on which the term info is written.
     */
    public void writeToFile(RandomAccessFile skipBlocksFile){

        /*// TODO: 08/05/2023 Scrivere il writeToFile per questo, e in merger il write to file di array list

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
        }*/
    }

    @Override
    public String toString() {
        return "startDocidOffset=" + startDocidOffset +
                "\nskipBlockDocidLength=" + skipBlockDocidLength +
                "\nstartFreqOffset=" + startFreqOffset +
                "\nskipBlockFreqLength=" + skipBlockFreqLength +
                "\nmaxDocid=" + maxDocid + "\n";
    }
}
