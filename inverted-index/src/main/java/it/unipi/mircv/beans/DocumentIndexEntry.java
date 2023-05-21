package it.unipi.mircv.beans;

import it.unipi.mircv.utils.Utils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
/**
 * Class that represents an entry of the document index, it contains the docno and the document length. The class
 * contains methods to write the document index into a random access file.
 */
public class DocumentIndexEntry {

    //Docno of the document
    private final String docNo;

    //Length of the document
    private final int docLength;

    //Length in bytes of the docno field
    public static int DOCNO_LENGTH = 48;

    //Length in bytes of the docLength field
    public static int DOCLENGTH_LENGTH = 4;

    //Length in bytes of the docId
    public static int DOCID_LENGTH = 8;

    //long + string[48] + int
    public static int DOCUMENT_INDEX_ENTRY_LENGTH = DOCID_LENGTH + DOCNO_LENGTH + DOCLENGTH_LENGTH;

    /**
     * Constructor
     * @param docNo docno of the document
     * @param docLength length in number of words that the document contains, in case of stemming it is the number of
     *                  stemmed words
     */
    public DocumentIndexEntry(String docNo, int docLength) {
        this.docNo = docNo;
        this.docLength = docLength;
    }

    public String getDocNo() {
        return docNo;
    }

    public int getDocLength() {
        return docLength;
    }

    /**
     * Writes the document index entry to disk
     * @param randomAccessFile the file to write to
     * @param docId the id of the document
     */
    public void writeToDisk(RandomAccessFile randomAccessFile, int docId){

        //Fill with whitespaces to keep the length standard
        String tmp = Utils.leftpad(this.docNo, DOCNO_LENGTH);

        //Instantiating the ByteBuffer to write to the file
        byte[] docIdBytes = ByteBuffer.allocate(DOCID_LENGTH).putLong(docId).array();
        byte[] docNoBytes = ByteBuffer.allocate(DOCNO_LENGTH).put(tmp.getBytes()).array();
        byte[] docLenBytes = ByteBuffer.allocate(DOCLENGTH_LENGTH).putInt(this.docLength).array();

        try {
            randomAccessFile.write(docIdBytes);
            randomAccessFile.write(docNoBytes);
            randomAccessFile.write(docLenBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Read from the document index the document index entry related to the given doc id
     * @param documentIndexFile random access file containing the document index
     * @param docId document id of which we want to retrieve the entry
     * @return the document index entry associated to the doc id
     */
    public static int getDocLenFromDisk(RandomAccessFile documentIndexFile, long docId){

        //Accumulator for the current offset in the file
        long offset = docId*DOCUMENT_INDEX_ENTRY_LENGTH+DOCID_LENGTH+DOCNO_LENGTH;


        try {
            //Move to the correct offset
            documentIndexFile.seek(offset);

            //Read the length of the document, 4 bytes starting from the offset
            return documentIndexFile.readInt();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public String toString() {
        return "[ "+ docNo + ", " + docLength + ']';
    }

    public static void main(String[] args){
        try (RandomAccessFile documentIndexFile = new RandomAccessFile(DocumentIndex.DOCUMENT_INDEX_PATH, "r")) {
            System.out.println(getDocLenFromDisk(documentIndexFile,0));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
