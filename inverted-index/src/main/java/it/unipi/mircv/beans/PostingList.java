package it.unipi.mircv.beans;

import it.unipi.mircv.compressor.Compressor;

import java.io.FileNotFoundException;
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

    //Iterator to iterate over the skip blocks
    private Iterator<SkipBlock> skipBlocksIterator;

    //Path of docids file
    private final static String DOCIDS_PATH = "Files/docids.txt";

    //Path of frequencies' file
    private final static String FREQUENCIES_PATH = "Files/frequencies.txt";

    //Path of skip blocks' file
    private final static String SKIP_BLOCKS_PATH = "Files/skipblocks.txt";

    //TermInfo of the term, used to retrieve the idf
    private TermInfo termInfo;

    //
    private SkipBlock currentSkipBlock;

    //Skip blocks of the posting list
    private ArrayList<SkipBlock> skipBlocks; // TODO: 10/05/2023 To use in nextGEQ

    private Configuration configuration;

    RandomAccessFile randomAccessFileDocIds;
    RandomAccessFile randomAccessFileFrequencies;
    RandomAccessFile randomAccessFileSkipBlocks;


    /**
     * Constructor
     */
    public PostingList() {
        super();
        noMorePostings = false;
    }

    public void openList(TermInfo termInfo, boolean queryType) {
        if(queryType){
            openListDisjunctive(termInfo);
        } else{
            openListConjunctive(termInfo);
        }
    }

    /**
     * Loads the posting list of the given term in memory, here the skipping is not needed.
     * @param termInfo Lexicon entry of the term, used to retrieve the offsets and the lengths of the posting list
     */
    private void openListDisjunctive(TermInfo termInfo){

        this.termInfo = termInfo;

        configuration = new Configuration();
        configuration.loadConfiguration();

        //Open the stream with the posting list files
        try(    RandomAccessFile randomAccessFileDocIds = new RandomAccessFile(DOCIDS_PATH, "r");
                RandomAccessFile randomAccessFileFrequencies = new RandomAccessFile(FREQUENCIES_PATH, "r")
                ){

            //Retrieve the docids and the frequencies
            ArrayList<Long> docids;
            ArrayList<Integer> frequencies;

            //If the compression is enabled, then read the posting lists files with the compression
            if(configuration.getCompressed()) {

                docids = readPostingListDocIdsCompressed(randomAccessFileDocIds, termInfo.getOffsetDocId(), termInfo.getDocIdsBytesLength());
                frequencies = readPostingListFrequenciesCompressed(randomAccessFileFrequencies, termInfo.getOffsetFrequency(), termInfo.getFrequenciesBytesLength());
            }else {//Read without compression

                docids = readPostingListDocIds(randomAccessFileDocIds,termInfo.getOffsetDocId(),termInfo.getDocIdsBytesLength());
                frequencies = readPostingListFrequencies(randomAccessFileFrequencies, termInfo.getOffsetFrequency(), termInfo.getFrequenciesBytesLength());
            }

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
     * Loads the posting list of the given term in memory, this list uses the skipping mechanism.
     * @param termInfo Lexicon entry of the term, used to retrieve the offsets and the lengths of the posting list
     */
    private void openListConjunctive(TermInfo termInfo){

        this.termInfo = termInfo;

        configuration = new Configuration();
        configuration.loadConfiguration();

        //Open the stream with the posting list files
        try {
            randomAccessFileDocIds = new RandomAccessFile(DOCIDS_PATH, "r");
            randomAccessFileFrequencies = new RandomAccessFile(FREQUENCIES_PATH, "r");
            randomAccessFileSkipBlocks = new RandomAccessFile(SKIP_BLOCKS_PATH,"r");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }


        //Load the skip blocks list of the current term's posting list
        skipBlocks = readPostingListSkipBlocks(
                randomAccessFileSkipBlocks,
                termInfo.getOffsetSkipBlock(),
                termInfo.getNumberOfSkipBlocks()
        );

        //initialize the skip blocks iterator
        skipBlocksIterator = skipBlocks.iterator();

        //move the skip blocks'0 iterator to the first skip block
        nextSkipBlock();

        //Load the posting list of the current block
        loadPostingList();

        //System.out.println(this);

        //System.out.println(skipBlocks.get(0));

    }

    public void loadPostingList(){
        //Retrieve the docids and the frequencies
        ArrayList<Long> docids;
        ArrayList<Integer> frequencies;

        //If the compression is enabled, then read the posting lists files with the compression
        if(configuration.getCompressed()) {

            docids = readPostingListDocIdsCompressed(randomAccessFileDocIds,
                    termInfo.getOffsetDocId() + currentSkipBlock.startDocidOffset,
                    currentSkipBlock.skipBlockDocidLength);

            frequencies = readPostingListFrequenciesCompressed(randomAccessFileFrequencies,
                    termInfo.getOffsetFrequency() + currentSkipBlock.startFreqOffset,
                    currentSkipBlock.skipBlockFreqLength);
        }else {//Read without compression

            docids = readPostingListDocIds(randomAccessFileDocIds,termInfo.getOffsetDocId(),skipBlocks.get(0).skipBlockDocidLength);
            frequencies = readPostingListFrequencies(randomAccessFileFrequencies, termInfo.getOffsetFrequency(), skipBlocks.get(0).skipBlockFreqLength);
        }

        //Remove the previous postings
        this.clear();

        //Create the array list of postings
        for(int i = 0; i < docids.size() ; i++){
            this.add(new Posting(docids.get(i), frequencies.get(i)));
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
    public void nextSkipBlock(){
        currentSkipBlock = skipBlocksIterator.next();
    }

    public Posting nextGEQ(long searchedDocId){

        while(currentSkipBlock.maxDocid < searchedDocId){
            System.out.println(currentSkipBlock.maxDocid +" < "+ searchedDocId);
            if(skipBlocksIterator.hasNext()){
                System.out.println("changing the skip block");
                nextSkipBlock();
            }else{

                System.out.println("end of posting list");
                setNoMorePostings();
                return null;
            }
        }

        //load the posting lists related to the current skip block
        loadPostingList();

        Posting posting;

        while(iterator.hasNext()){
            posting = next();
            if(this.docId >= searchedDocId){
                System.out.println(this.docId + "stopped here");
                return posting;
            }
        }

        return null;
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
    public void closeList(){ // TODO: 10/05/2023 Add to the end of the query processing for the term
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

    public SkipBlock getCurrentSkipBlock() {
        return currentSkipBlock;
    }

    public Iterator<SkipBlock> getSkipBlocksIterator() {
        return skipBlocksIterator;
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

    /**
     * Reads the posting list's skip blocks from the given file, starting from offset it will read the
     * number of skip blocks indicated by the given length parameter.
     * @param randomAccessFileSkipBlocks RandomAccessFile of the skip blocks' file
     * @param offset offset starting from where to read the skip blocks'
     * @param length number of skip blocks to read
     */
    public static ArrayList<SkipBlock> readPostingListSkipBlocks(RandomAccessFile randomAccessFileSkipBlocks, long offset, int length) {

        //ArrayList to store the posting list's frequencies
        ArrayList<SkipBlock> list = new ArrayList<>();

        try {

            //Set the file pointer to the start of the posting list
            randomAccessFileSkipBlocks.seek(offset);

        } catch (IOException e) {
            System.err.println("[ReadPostingListSkipBlocks] Exception during seek");
            throw new RuntimeException(e);
        }

        //Read the skip blocks from the file
        for(int i = 0; i < length; i ++) {
            try {

                //Read the next skip block from the file and add it to the result list
                list.add(new SkipBlock(
                        randomAccessFileSkipBlocks.readLong(), //Docids offset
                        randomAccessFileSkipBlocks.readInt(),  //Docids length
                        randomAccessFileSkipBlocks.readLong(), //Frequencies offset
                        randomAccessFileSkipBlocks.readInt(),  //Frequencies length
                        randomAccessFileSkipBlocks.readLong()) //Max docid in the skip block
                );

            } catch (IOException e) {
                System.err.println("[ReadPostingListSkipBlocks] Exception during read");
                throw new RuntimeException(e);
            }
        }

        //Return the list
        return list;
    }
}