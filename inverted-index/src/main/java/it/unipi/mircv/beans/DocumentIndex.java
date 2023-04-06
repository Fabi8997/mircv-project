package it.unipi.mircv.beans;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.util.HashMap;

public class DocumentIndex extends HashMap<Long, DocumentIndexEntry> {

    //Path to the document index file
    private final static String DOCUMENT_INDEX_PATH = "Files/document_index.txt";

    /**
     * Construct that calls the HashMap constructor
     */
    public DocumentIndex(){
        super();
    }

    /**
     * Load the document index in memory.
     */
    public void loadDocumentIndex() {
        System.out.println("[DOCUMENT INDEX LOADER] Document index loading");
        try (//Object to open the stream from the document index file
             RandomAccessFile documentIndexFile = new RandomAccessFile(DOCUMENT_INDEX_PATH, "r")){
            //Start the stream from the document index file

            //Accumulator for the current offset in the file
            int offset = 0;

            //Array of bytes in which put the docno
            byte[] docnoBytes = new byte[DocumentIndexEntry.DOCNO_LENGTH];

            long docid;

            int docLength;

            String docno;

            //System.out.println(documentIndexFile.length());
            //While we're not at the end of the file
            while (offset < documentIndexFile.length()) {

                //Read the docid from the first 8 bytes starting from the offset
                docid = documentIndexFile.readLong();

                //Read the first DOCUMENT_INDEX_DOCNO_LENGTH bytes containing the docno
                documentIndexFile.readFully(docnoBytes, 0, DocumentIndexEntry.DOCNO_LENGTH);

                //Convert the bytes to a string and trim it
                docno = new String(docnoBytes, Charset.defaultCharset()).trim();

                //Read the length of the document, 4 bytes starting from the offset
                docLength = documentIndexFile.readInt();

                //Insert the termInfo into the HashMap
                this.put(docid, new DocumentIndexEntry(docno,docLength));

                //Increment the offset
                offset += DocumentIndexEntry.DOCUMENT_INDEX_ENTRY_LENGTH;
            }

            System.out.println("[DOCUMENT INDEX LOADER] Document index loaded");

        } catch (Exception e) {
            System.out.println("[DOCUMENT INDEX LOADER] Error loading the document index: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
