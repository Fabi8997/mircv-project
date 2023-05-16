package it.unipi.mircv.beans;

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

    static final int OFFSET_LENGTH = 8;

    static final int SKIP_BLOCK_DIMENSION_LENGTH = 4;

    static final int MAX_DOC_ID_LENGTH = 8;

    //Length in byte of each skip block (32)
    public static final int SKIP_BLOCK_LENGTH = 2*OFFSET_LENGTH + 2*SKIP_BLOCK_DIMENSION_LENGTH + MAX_DOC_ID_LENGTH;

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

    public SkipBlock(long startDocidOffset, int skipBlockDocidLength, long startFreqOffset, int skipBlockFreqLength, long maxDocid) {
        this.startDocidOffset = startDocidOffset;
        this.skipBlockDocidLength = skipBlockDocidLength;
        this.startFreqOffset = startFreqOffset;
        this.skipBlockFreqLength = skipBlockFreqLength;
        this.maxDocid = maxDocid;
    }

    public void setFreqInfo(long startFreqOffset, int skipBlockFreqLength) {
        this.startFreqOffset = startFreqOffset;
        this.skipBlockFreqLength = skipBlockFreqLength;
    }

    /**
     * Write the term info to a file. This method is used during the merge of the partial blocks, here we have
     * all the information directly inside the termInfo object.
     * @param skipBlocksFile Is the random access file on which the term info is written.
     */
    public void writeToFile(RandomAccessFile skipBlocksFile){
        byte[] startDocIdOffset = ByteBuffer.allocate(OFFSET_LENGTH).putLong(this.startDocidOffset).array();
        byte[] skipBlockDocIdLength = ByteBuffer.allocate(SKIP_BLOCK_DIMENSION_LENGTH).putInt(this.skipBlockDocidLength).array();
        byte[] startFreqOffset = ByteBuffer.allocate(OFFSET_LENGTH).putLong(this.startFreqOffset).array();
        byte[] skipBlockFreqLength = ByteBuffer.allocate(SKIP_BLOCK_DIMENSION_LENGTH).putInt(this.skipBlockFreqLength).array();
        byte[] maxDocId = ByteBuffer.allocate(MAX_DOC_ID_LENGTH).putLong(this.maxDocid).array();
        try {
            skipBlocksFile.write(startDocIdOffset);
            skipBlocksFile.write(skipBlockDocIdLength);
            skipBlocksFile.write(startFreqOffset);
            skipBlocksFile.write(skipBlockFreqLength);
            skipBlocksFile.write(maxDocId);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
