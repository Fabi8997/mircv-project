package it.unipi.mircv.beans;

import it.unipi.mircv.compressor.Compressor;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * This class represents a posting list, it is implemented as an arrayList of postings; it provides methods to load
 * the posting list of a given term into memory and iterate over it.
 */
public class PostingList extends ArrayList<Posting> {

    //Current docid
    private long docId;

    //Current frequency
    private int frequency;

    //If we've reached the end of the posting list
    private boolean noMorePostings;

    //Iterator to iterate over the posting list
    private Iterator<Posting> iterator;

    //Path of docids file
    private final static String DOCIDS_PATH = "Files/docids.txt";

    //Path of frequencies' file
    private final static String FREQUENCIES_PATH = "Files/frequencies.txt";

    //TermInfo of the term, used to retrieve the idf
    private TermInfo termInfo;
    /**
     * Constructor
     */
    public PostingList() {
        super();
        noMorePostings = false;
    }

    /**
     * Loads the posting list of the given term in memory
     * @param termInfo Lexicon entry of the term, used to retrieve the offsets and the lengths of the posting list
     */
    public void openList(TermInfo termInfo){

        this.termInfo = termInfo;

        // TODO: 05/04/2023 Retrieve if the compression is active!
        // boolean compression;

        //Open the stream with the posting list files
        try(    RandomAccessFile randomAccessFileDocIds = new RandomAccessFile(DOCIDS_PATH, "r");
                RandomAccessFile randomAccessFileFrequencies = new RandomAccessFile(FREQUENCIES_PATH, "r")
                ){

            // TODO: 05/04/2023 if compression, otherwise the other read
            //Retrieve the docids and the frequencies
            ArrayList<Long> docids = readPostingListDocIdsCompressed(randomAccessFileDocIds, termInfo.getOffsetDocId(), termInfo.getDocIdsBytesLength());
            ArrayList<Integer> frequencies = readPostingListFrequenciesCompressed(randomAccessFileFrequencies, termInfo.getOffsetFrequency(), termInfo.getFrequenciesBytesLength());

            //Create the array list of postings
            for(int i = 0; i < termInfo.getPostingListLength(); i++){
                this.add(new Posting(docids.get(i), frequencies.get(i)));
            }
        }catch (IOException e){
            System.err.println("[OpenList] Exception during opening posting list");
            throw new RuntimeException(e);
        }

        iterator = this.iterator();
    }

    /**
     * Moves the iterator to the next posting in the iteration
     * @return the next posting
     */
    public Posting next(){

        //Get the next posting in the iteration
        Posting result = iterator.next();

        //Update the current information
        this.docId = result.docId;
        this.frequency = result.frequency;

        //Return the next
        return result;
    }

    /**
     * Returns true if the iteration has more elements.
     * (In other words, returns true if next would return an element rather than throwing an exception.)
     * @return true if the iteration has more elements.
     */
    public boolean hasNext(){
        return iterator.hasNext();
    }

    /**
     * Clear the array list
     */
    public void closeList(){
        this.clear();
    }

    /**
     * Get the current docid.
     * @return the current docid.
     */
    public long getDocId(){
        return docId;
    }

    /**
     * Get the current frequency.
     * @return the current frequency.
     */
    public int getFreq(){
        return frequency;
    }

    /**
     * Get if the current posting list has no more postings.
     * @return true if the current posting list has no more postings, false otherwise.
     */
    public boolean noMorePostings() {
        return noMorePostings;
    }

    /**
     * Set to true the flag to signal that no more postings are available.
     */
    public void setNoMorePostings() {
        this.noMorePostings = true;
    }

    public TermInfo getTermInfo() {
        return termInfo;
    }


    /**
     * Reads the posting list's ids from the given inverted index file, starting from offset it will read the number
     * of docIds indicated by the given length parameter. It assumes that the file is compressed using VBE.
     * @param randomAccessFileDocIds RandomAccessFile of the docIds block file
     * @param offset offset starting from where to read the posting list
     * @param length length of the bytes of the encoded posting list
     */
    public static ArrayList<Long> readPostingListDocIdsCompressed(RandomAccessFile randomAccessFileDocIds, long offset, int length) {

        byte[] docidsByte = new byte[length];

        try {

            //Set the file pointer to the start of the posting list
            randomAccessFileDocIds.seek(offset);

            randomAccessFileDocIds.readFully(docidsByte, 0, length);

            return Compressor.variableByteDecodeLong(docidsByte);

        } catch (IOException e) {
            System.err.println("[ReadPostingListDocIds] Exception during seek");
            throw new RuntimeException(e);
        }
    }


    /**
     * Reads the posting list's frequencies from the given inverted index file, starting from offset it will read the number
     * of docIds indicated by the given length parameter. It assumes that the file is compressed using VBE.
     * @param randomAccessFileFreq RandomAccessFile of the frequencies file
     * @param offset offset starting from where to read the posting list
     * @param length length of the bytes of the encoded posting list
     */
    public static ArrayList<Integer> readPostingListFrequenciesCompressed(RandomAccessFile randomAccessFileFreq, long offset, int length) {

        byte[] docidsByte = new byte[length];

        try {

            //Set the file pointer to the start of the posting list
            randomAccessFileFreq.seek(offset);

            randomAccessFileFreq.readFully(docidsByte, 0, length);

            return Compressor.variableByteDecode(docidsByte);

        } catch (IOException e) {
            System.err.println("[ReadPostingListDocIds] Exception during seek");
            throw new RuntimeException(e);
        }
    }

    /**
     * Reads the posting list's ids from the given inverted index file, starting from offset it will read the number
     * of docIds indicated by the given length parameter.
     * @param randomAccessFileDocIds RandomAccessFile of the docIds block file
     * @param offset offset starting from where to read the posting list
     * @param length length of the posting list to be read
     */
    public static ArrayList<Long> readPostingListDocIds(RandomAccessFile randomAccessFileDocIds, long offset, int length) {

        //ArrayList to store the posting list's ids
        ArrayList<Long> list = new ArrayList<>();

        try {

            //Set the file pointer to the start of the posting list
            randomAccessFileDocIds.seek(offset);

        } catch (IOException e) {
            System.err.println("[ReadPostingListDocIds] Exception during seek");
            throw new RuntimeException(e);
        }

        //Read the docIds from the file
        for(int i = 0; i < length; i ++) {
            try {

                //Read the docId and add it to the list
                list.add(randomAccessFileDocIds.readLong());

            } catch (IOException e) {
                System.err.println("[ReadPostingListDocIds] Exception during read");
                throw new RuntimeException(e);
            }
        }

        //Return the list
        return list;
    }

    /**
     * Reads the posting list's frequencies from the given inverted index file, starting from offset it will read the
     * number of frequencies indicated by the given length parameter.
     * @param randomAccessFileFrequencies RandomAccessFile of the frequencies block file
     * @param offset offset starting from where to read the posting list
     * @param length length of the posting list to be read
     */
    public static ArrayList<Integer> readPostingListFrequencies(RandomAccessFile randomAccessFileFrequencies, long offset, int length) {

        //ArrayList to store the posting list's frequencies
        ArrayList<Integer> list = new ArrayList<>();

        try {

            //Set the file pointer to the start of the posting list
            randomAccessFileFrequencies.seek(offset);

        } catch (IOException e) {
            System.err.println("[ReadPostingListFrequencies] Exception during seek");
            throw new RuntimeException(e);
        }

        //Read the frequencies from the file
        for(int i = 0; i < length; i ++) {
            try {

                //Read the frequency and add it to the list
                list.add(randomAccessFileFrequencies.readInt());

            } catch (IOException e) {
                System.err.println("[ReadPostingListFrequencies] Exception during read");
                throw new RuntimeException(e);
            }
        }

        //Return the list
        return list;
    }
}