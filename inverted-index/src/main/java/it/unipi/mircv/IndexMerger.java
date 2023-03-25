package it.unipi.mircv;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedList;


public class IndexMerger {

    final static String INVERTED_INDEX_DOC_IDS_BLOCK_PATH = "src/main/resources/files/invertedIndexDocIds";
    final static String INVERTED_INDEX_FREQUENCIES_BLOCK_PATH = "src/main/resources/files/invertedIndexFrequencies";
    final static String LEXICON_BLOCK_PATH = "src/main/resources/files/lexiconBlock";

    final static String LEXICON_PATH = "src/main/resources/files/lexicon.txt";

    final static String INVERTED_INDEX_PATH = "src/main/resources/files/invertedIndex.txt";

    public static void merge() {
        //Retrieve the blocks statistics
        Statistics statistics = readStatistics();

        //Arrays of random access files, for docIds, frequencies and lexicon blocks
        RandomAccessFile[] randomAccessFileDocIds = new RandomAccessFile[statistics.numberOfBlocks];
        RandomAccessFile[] randomAccessFilesFrequencies = new RandomAccessFile[statistics.numberOfBlocks];
        RandomAccessFile[] randomAccessFilesLexicon = new RandomAccessFile[statistics.numberOfBlocks];

        //Array of the current offset reached in each lexicon block
        int[] offsets = new int[statistics.numberOfBlocks];

        //Set each offset equal to 0, the starting offset of each lexicon block
        for(int i = 0; i < statistics.numberOfBlocks; i++) {
            offsets[i] = 0;
        }

        //Array of boolean, each i-th entry is true, if the i-th block has reached the end of the lexicon block file
        boolean[] endOfBlock = new boolean[statistics.numberOfBlocks];

        //Set each boolean equal to false, at the beginning no block has reached the end
        for (int i = 0; i < statistics.numberOfBlocks; i++) {
            endOfBlock[i] = false;
        }

        //String to keep the min term among all the current terms in each lexicon block, it is used to determine the
        // term of which the posting lists must be merged
        String minTerm = null;

        //Used to store the information of a term entry from the lexicon block file
        TermInfo curTerm;

        //Contains the list of all the blocks containing the current min term
        LinkedList<Integer> blocksWithMinTerm = new LinkedList<>();

        try {
            //Create a stream for each random access files of each block, the stream is opened ad read only
            for (int i = 0; i < statistics.numberOfBlocks; i++) {
                randomAccessFileDocIds[i] = new RandomAccessFile(INVERTED_INDEX_DOC_IDS_BLOCK_PATH+(i+1)+".txt", "r");
                randomAccessFilesFrequencies[i] = new RandomAccessFile(INVERTED_INDEX_FREQUENCIES_BLOCK_PATH+(i+1)+".txt", "r");
                randomAccessFilesLexicon[i] = new RandomAccessFile(LEXICON_BLOCK_PATH+(i+1)+".txt", "r");
            }
        } catch (FileNotFoundException e) {
            System.err.println("[MERGER] File not found: " + e.getMessage());
            throw new RuntimeException(e);
        }

        //Iterate over all the lexicon blocks, until the end of the lexicon block file is reached for each block
        while(!endOfAllFiles(endOfBlock, statistics.numberOfBlocks)) {

            System.out.println("[MERGER] Search the current min term in the lexicon block files");

            //For each block read the next term without moving the pointer of the blocks
            for(int i = 0; i < statistics.numberOfBlocks; i++) {

                //Avoid to read from the block if the end of the block is reached
                if(endOfBlock[i]) {
                    continue;
                }

                //Read the current term in the lexicon block
                curTerm = readNextTermInfo(randomAccessFilesLexicon[i],offsets[i],true);

                //Check if the end of the block is reached
                if(curTerm == null) {

                    //Set the end of the block i to true
                    endOfBlock[i] = true;
                    continue;
                }

                //If the current term is the lexicographically smaller than the min term, then update the min term.
                if(minTerm == null || curTerm.getTerm().compareTo(minTerm) < 0) {

                    //If we've found another min term, then update the min term.
                    minTerm = curTerm.getTerm();

                    //Clear the array of blocks with the min term.
                    blocksWithMinTerm.clear();

                    //Add the current block to the list of blocks with the min term.
                    blocksWithMinTerm.add(i);

                    //Else if the current term is equal to the min term, then add the current block to the list of blocks with the min term.
                } else if (curTerm.getTerm().compareTo(minTerm) == 0) {

                    //Add the current block to the list of blocks with the min term.
                    blocksWithMinTerm.add(i);
                }
            }//At this point we have the current min term.

            //Check if we've reached the and of the merge.
            if(endOfAllFiles(endOfBlock, statistics.numberOfBlocks)) {
                System.out.println("END OF ALL FILES");
                break;
            }

            System.out.println("----------- TERM: " + minTerm + " -----------");
            System.out.println(blocksWithMinTerm);

            //Term info to retrieve the information about the posting lists of the current min term in the current block
            TermInfo currentTermInfo;

            //Array to store the docIds and frequencies of the posting list of the current min term in the current block
            ArrayList<Integer> docIds = new ArrayList<>();
            ArrayList<Integer> frequencies = new ArrayList<>();

            //Merge the posting lists of the current min term in the blocks containing the term
            for (Integer integer : blocksWithMinTerm) {
                System.out.println("Block " + integer + ":");

                //Read the lexicon entry from the current block and move the pointer of the file to the next term
                currentTermInfo = readNextTermInfo(randomAccessFilesLexicon[integer], offsets[integer], false);

                //Check if the end of the block is reached or a problem during the reading occurred
                if(currentTermInfo == null) {
                    continue;
                }

                //Increment the offset of the current block to the starting offset of the next term
                offsets[integer] += 60;

                //For debug, to be deleted
                System.out.println("DOC-ID-"+integer+": " + readPostingListDocIds(randomAccessFileDocIds[integer], currentTermInfo.getOffsetDocId(), currentTermInfo.getPostingListLength()));
                System.out.println("FREQ-"+integer+": " + readPostingListFrequencies(randomAccessFilesFrequencies[integer], currentTermInfo.getOffsetFrequency(), currentTermInfo.getPostingListLength()));

                //Append the current term docIds to the docIds accumulator
                docIds.addAll(readPostingListDocIds(randomAccessFileDocIds[integer], currentTermInfo.getOffsetDocId(), currentTermInfo.getPostingListLength()));

                //Append the current term frequencies to the frequencies accumulator
                frequencies.addAll(readPostingListFrequencies(randomAccessFilesFrequencies[integer], currentTermInfo.getOffsetFrequency(), currentTermInfo.getPostingListLength()));
            }

            // TODO: 25/03/2023 Instead of printing, write to a file in a compressed form.
            System.out.println("DocIds-merged:" + docIds);
            System.out.println("Frequencies-merged" + frequencies);

            // TODO: 25/03/2023 Implement the compression algorithm
            //System.out.println(Arrays.toString(offsets));

            System.out.println("----------------------");

            //Clear the accumulators for the next iteration
            minTerm = null; //Otherwise it will be always the first min term found at the beginning of the merge
            blocksWithMinTerm.clear(); //Clear the list of blocks with the min term
        }

        // TODO: 25/03/2023 Close the files.

        System.out.println("END OF MERGE");
    }


    /**
     * Reads the next lexicon entry from the given lexicon block file, starting from offset it will read the first 60
     * bytes, then if resetOffset is true, it will reset the offset to the value present ate the beginning, otherwise it
     * will keep the cursor as it is after the read of the entry.
     * @param randomAccessFileLexicon RandomAccessFile of the lexicon block file
     * @param offset offset starting from where to read the lexicon entry
     * @param resetOffset boolean indicating whether to reset the offset to the value present before the read of the
     *                    entry
     */
    public static TermInfo readNextTermInfo(RandomAccessFile randomAccessFileLexicon, int offset, boolean resetOffset) {

        //Array of bytes in which put the term
        byte[] termBytes = new byte[48];

        //String containing the term
        String term;

        //TermInfo containing the term information to be returned
        TermInfo termInfo;

        try {
            //Set the file pointer to the start of the lexicon entry
            randomAccessFileLexicon.seek(offset);

            //Read the first 48 containing the term
            randomAccessFileLexicon.readFully(termBytes, 0, 48);

            //Convert the bytes to a string and trim it
            term = new String(termBytes, Charset.defaultCharset()).trim();

            //Instantiate the TermInfo object reading the next 3 integers from the file
            termInfo = new TermInfo(term, randomAccessFileLexicon.readInt(), randomAccessFileLexicon.readInt(), randomAccessFileLexicon.readInt());

            if(resetOffset)
                //Reset the offset to the value present before the read of the entry
                randomAccessFileLexicon.seek(offset);

            return termInfo;

        } catch (IOException e) {
            System.err.println("[ReadNextTermInfo] EOF reached while reading the next lexicon entry");
            return null;
        }
    }

    /**
     * Reads the posting list's ids from the given inverted index file, starting from offset it will read the number
     * of docIds indicated by the given length parameter.
     * @param randomAccessFileDocIds RandomAccessFile of the docIds block file
     * @param offset offset starting from where to read the posting list
     * @param length length of the posting list to be read
     */
    private static ArrayList<Integer> readPostingListDocIds(RandomAccessFile randomAccessFileDocIds, int offset, int length) {

        //ArrayList to store the posting list's ids
        ArrayList<Integer> list = new ArrayList<>();

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
                list.add(randomAccessFileDocIds.readInt());

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
    private static ArrayList<Integer> readPostingListFrequencies(RandomAccessFile randomAccessFileFrequencies, int offset, int length) {

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
     * Return a statistics object containing the information about the blocks
     */
    private static Statistics readStatistics(){
        return new Statistics();
    }

    /**
     * Check if all the files have reached the end of the file, and if so return true, otherwise return false
     * @param endOfBlocks array of boolean indicating if the files have reached the end of the file
     * @param numberOfBlocks number of blocks, it is the length of the array
     * @return true if all the files have reached the end of the file, and if so return true, otherwise return false
     */
    private static boolean endOfAllFiles(boolean[] endOfBlocks, int numberOfBlocks) {

        for(int i = 0; i < numberOfBlocks; i++) {
            if(!endOfBlocks[i])
                return false;
        }
        return true;
    }

    public static void main(String[] args){
        merge();
    }
}
