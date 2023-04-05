package it.unipi.mircv.merger;

import it.unipi.mircv.beans.PostingList;
import it.unipi.mircv.beans.Statistics;
import it.unipi.mircv.beans.TermInfo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedList;

import static it.unipi.mircv.compressor.Compressor.*;


public class IndexMerger {
    final static String INVERTED_INDEX_DOC_IDS_BLOCK_PATH = "inverted-index/src/main/resources/tmp/invertedIndexDocIds";
    final static String INVERTED_INDEX_FREQUENCIES_BLOCK_PATH = "inverted-index/src/main/resources/tmp/invertedIndexFrequencies";
    final static String LEXICON_BLOCK_PATH = "inverted-index/src/main/resources/tmp/lexiconBlock";
    final static String LEXICON_PATH = "Files/lexicon.txt";
    final static String INVERTED_INDEX_DOC_IDS_PATH = "Files/docids.txt";
    final static String INVERTED_INDEX_FREQUENCIES_PATH = "Files/frequencies.txt";

    /**
     * This method merges the inverted index and the lexicon blocks into one single file.
     * @param compress If true, the inverted index and the lexicon blocks will be compressed using VBE, otherwise
     *                 they will be written without compression.
     */
    public static void merge(boolean compress) {

        System.out.println("[MERGER] Merging lexicon blocks and inverted index blocks...");

        //Retrieve the time at the beginning of the computation
        long begin = System.nanoTime();

        //Retrieve the blocks statistics
        Statistics statistics = readStatistics();

        int NUMBER_OF_BLOCKS = statistics.getNumberOfBlocks();

        //Arrays of random access files, for docIds, frequencies and lexicon blocks
        RandomAccessFile[] randomAccessFileDocIds = new RandomAccessFile[NUMBER_OF_BLOCKS];
        RandomAccessFile[] randomAccessFilesFrequencies = new RandomAccessFile[NUMBER_OF_BLOCKS];
        RandomAccessFile[] randomAccessFilesLexicon = new RandomAccessFile[NUMBER_OF_BLOCKS];

        //Files for the final result
        RandomAccessFile lexiconFile;
        RandomAccessFile docIdsFile;
        RandomAccessFile frequenciesFile;

        //Accumulators to hold the current offset, starting from which the next list of postings will be written
        long docIdsOffset = 0;
        long frequenciesOffset = 0;

        //Array of the current offset reached in each lexicon block
        int[] offsets = new int[NUMBER_OF_BLOCKS];

        //Array of boolean, each i-th entry is true, if the i-th block has reached the end of the lexicon block file
        boolean[] endOfBlock = new boolean[NUMBER_OF_BLOCKS];

        //Set each offset equal to 0, the starting offset of each lexicon block
        //Set each boolean equal to false, at the beginning no block has reached the end
        for (int i = 0; i < NUMBER_OF_BLOCKS; i++) {
            offsets[i] = 0;
            endOfBlock[i] = false;
        }

        //String to keep the min term among all the current terms in each lexicon block, it is used to determine the
        // term of which the posting lists must be merged
        String minTerm = null;

        //TermInfo to keep the term's information to be written in the lexicon file
        TermInfo lexiconEntry;

        //Used to store the information of the current term entry for each lexicon block file
        TermInfo[] curTerm = new TermInfo[NUMBER_OF_BLOCKS];

        //Contains the list of all the blocks containing the current min term
        LinkedList<Integer> blocksWithMinTerm = new LinkedList<>();

        //Array to store the docIds and frequencies of the posting list of the current min term in the current block
        ArrayList<Long> docIds = new ArrayList<>();
        ArrayList<Integer> frequencies = new ArrayList<>();

        //Arrays to store the compressed docIds and frequencies of the posting list of the current min term
        byte[] docIdsCompressed;
        byte[] frequenciesCompressed;


        try {
            //Create a stream for each random access files of each block, the stream is opened ad read only
            for (int i = 0; i < NUMBER_OF_BLOCKS; i++) {
                randomAccessFileDocIds[i] = new RandomAccessFile(INVERTED_INDEX_DOC_IDS_BLOCK_PATH+(i+1)+".txt", "r");
                randomAccessFilesFrequencies[i] = new RandomAccessFile(INVERTED_INDEX_FREQUENCIES_BLOCK_PATH+(i+1)+".txt", "r");
                randomAccessFilesLexicon[i] = new RandomAccessFile(LEXICON_BLOCK_PATH+(i+1)+".txt", "r");
            }

            //Create a stream for the lexicon file, the docids file and the frequencies file, the stream is opened as write only
            lexiconFile = new RandomAccessFile(LEXICON_PATH, "rw");
            docIdsFile = new RandomAccessFile(INVERTED_INDEX_DOC_IDS_PATH, "rw");
            frequenciesFile = new RandomAccessFile(INVERTED_INDEX_FREQUENCIES_PATH, "rw");

        } catch (FileNotFoundException e) {
            System.err.println("[MERGER] File not found: " + e.getMessage());
            throw new RuntimeException(e);
        }

        //Read the first term of each lexicon block
        for (int i = 0; i < curTerm.length; i++) {
            curTerm[i] = readNextTermInfo(randomAccessFilesLexicon[i],offsets[i]);

            if(curTerm[i] == null) {
                endOfBlock[i] = true;
            }

            //Update the offset to the offset of the next file to be read
            offsets[i] += 68;
        }

        long j = 1;
        //Iterate over all the lexicon blocks, until the end of the lexicon block file is reached for each block
        while(!endOfAllFiles(endOfBlock, NUMBER_OF_BLOCKS)) {
            j++;
            if(j%25000 == 0){
                System.out.println("[MERGER] Processing time: " + (System.nanoTime() - begin)/1000000000+ "s. Processed " + j + " terms");
            }

            //System.out.println("[MERGER] Search the current min term in the lexicon block files");

            //For each block read the next term without moving the pointer of the blocks
            for(int i = 0; i < NUMBER_OF_BLOCKS; i++) {

                //Avoid to read from the block if the end of the block is reached
                if(endOfBlock[i]) {
                    continue;
                }

                //If the current term is the lexicographically smaller than the min term, then update the min term.
                if(minTerm == null || curTerm[i].getTerm().compareTo(minTerm) < 0) {

                    //If we've found another min term, then update the min term.
                    minTerm = curTerm[i].getTerm();

                    //Clear the array of blocks with the min term.
                    blocksWithMinTerm.clear();

                    //Add the current block to the list of blocks with the min term.
                    blocksWithMinTerm.add(i);

                    //Else if the current term is equal to the min term, then add the current block to the list of blocks with the min term.
                } else if (curTerm[i].getTerm().compareTo(minTerm) == 0) {

                    //Add the current block to the list of blocks with the min term.
                    blocksWithMinTerm.add(i);
                }
            }//At this point we have the current min term.

            //Check if we've reached the and of the merge.
            if(endOfAllFiles(endOfBlock, NUMBER_OF_BLOCKS)) {
                System.out.println("END OF ALL FILES");
                break;
            }

            //System.out.println("----------- TERM: " + minTerm + " -----------");
            //System.out.println(blocksWithMinTerm);

            //Merge the posting lists of the current min term in the blocks containing the term
            for (Integer integer : blocksWithMinTerm) {

                //Append the current term docIds to the docIds accumulator
                docIds.addAll(PostingList.readPostingListDocIds(randomAccessFileDocIds[integer], curTerm[integer].getOffsetDocId(), curTerm[integer].getPostingListLength()));

                //System.out.println("Current docIds: " + docIds);

                //Append the current term frequencies to the frequencies accumulator
                frequencies.addAll(PostingList.readPostingListFrequencies(randomAccessFilesFrequencies[integer], curTerm[integer].getOffsetFrequency(), curTerm[integer].getPostingListLength()));

                //System.out.println("Current term frequencies: " + frequencies);

                //Read the lexicon entry from the current block and move the pointer of the file to the next term
                curTerm[integer] = readNextTermInfo(randomAccessFilesLexicon[integer], offsets[integer]);

                //Check if the end of the block is reached or a problem during the reading occurred
                if(curTerm[integer] == null) {
                    System.out.println("[MERGER] Block " + integer + " has reached the end of the file");
                    endOfBlock[integer] = true;
                    continue;
                }

                //Increment the offset of the current block to the starting offset of the next term
                offsets[integer] +=
                        TermInfo.TERM_LENGTH +
                        TermInfo.OFFSET_DOCIDS_LENGTH +
                        TermInfo.OFFSET_FREQUENCIES_LENGTH +
                        TermInfo.POSTING_LIST_LENGTH;

            }

            if(compress){
                //Compress the list of docIds using VBE
                docIdsCompressed = variableByteEncodeLong(docIds);

                //Compress the list of frequencies using VBE
                frequenciesCompressed = variableByteEncodeInt(frequencies);

                //Write the docIds and frequencies of the current term in the respective files
                try {
                    docIdsFile.write(docIdsCompressed);
                    frequenciesFile.write(frequenciesCompressed);
                } catch (IOException e) {
                    System.err.println("[MERGER] File not found: " + e.getMessage());
                    throw new RuntimeException(e);
                }

                double idf = Math.log(statistics.getNumberOfDocuments()/ (double)docIds.size())/Math.log(2);

                lexiconEntry = new TermInfo(
                        minTerm,                     //Term
                        docIdsOffset,                //offset in the docids file in which the docids list starts
                        frequenciesOffset,           //offset in the frequencies file in which the frequencies list starts
                        idf,                         //idf
                        docIdsCompressed.length,     //length in bytes of the compressed docids list
                        frequenciesCompressed.length,//length in bytes of the compressed frequencies list
                        docIds.size());              //Length of the posting list of the current term
                if(j%25000 == 0) {
                    System.out.println("[MERGER] idf = " + idf + ". TermInfo.idf = " + lexiconEntry.getIdf() + ". Term: " + lexiconEntry.getTerm());
                }
                if(minTerm.equals("dog") || minTerm.equals("ball") || minTerm.equals("sport")){
                    System.out.println("[MERGER] idf = " + idf + ". TermInfo.idf = " + lexiconEntry.getIdf() + ". Term: " + lexiconEntry.getTerm());
                }
                //terminfo.setTFIDF()
                //terminfo.setBM25()
                lexiconEntry.writeToFile(lexiconFile, lexiconEntry);

                docIdsOffset += docIdsCompressed.length;
                frequenciesOffset += frequenciesCompressed.length;


            }else {//No compression

                //Write the docIds and frequencies of the current term in the respective files
                try {

                    //Write the docIds as a long to the end of the docIds file
                    for (Long docId : docIds) {
                        docIdsFile.writeLong(docId);
                    }

                    //Write the frequencies as an integer to the end of the frequencies file
                    for (Integer frequency : frequencies) {
                        frequenciesFile.writeInt(frequency);
                    }
                } catch (IOException e) {
                    System.err.println("[MERGER] File not found: " + e.getMessage());
                    throw new RuntimeException(e);
                }

                double idf = Math.log(statistics.getNumberOfDocuments()/ (double)docIds.size())/Math.log(2);
                //Instantiate a new TermInfo object with the current term information, here we use the information in
                //the docids and frequencies objects
                lexiconEntry = new TermInfo(
                        minTerm,                     //Term
                        docIdsOffset,                //offset in the docids file in which the docids list starts
                        frequenciesOffset,           //offset in the frequencies file in which the frequencies list starts
                        idf,
                        docIds.size(),               //length in number of long in the docids list
                        frequencies.size(),          //length number of integers in the frequencies list
                        docIds.size());              //Length of the posting list of the current term

                //terminfo.setTFIDF()
                //terminfo.setBM25()
                lexiconEntry.writeToFile(lexiconFile, lexiconEntry);

                docIdsOffset += 8L*docIds.size();
                frequenciesOffset += 4L*frequencies.size();


            }

            //Clear the accumulators for the next iteration
            docIds.clear();
            frequencies.clear();
            minTerm = null; //Otherwise it will be always the first min term found at the beginning of the merge
            blocksWithMinTerm.clear(); //Clear the list of blocks with the min term
        }

        System.out.println("[MERGER] Closing the streams of the files. Analyzed " + j + " terms");

        try {
            //Close the streams of the files
            for (int i = 0; i < NUMBER_OF_BLOCKS; i++) {
                randomAccessFileDocIds[i].close();
                randomAccessFilesFrequencies[i].close();
                randomAccessFilesLexicon[i].close();
            }

            lexiconFile.close();
            docIdsFile.close();
            frequenciesFile.close();

        } catch (RuntimeException | IOException e) {
            System.err.println("[MERGER] File not found: " + e.getMessage());
            throw new RuntimeException(e);
        }

        System.out.println("[MERGER] Deleting the partial blocks !!!! REMOVE /**/ to make it work");

        /*if(deleteBlocks(NUMBER_OF_BLOCKS)){
            System.out.println("[MERGER] Blocks deleted successfully");
        }*/

        System.out.println("[MERGER] Total processing time: " + (System.nanoTime() - begin)/1000000000+ "s");
        System.out.println("[MERGER] MERGING PROCESS COMPLETE");
    }


    /**
     * Reads the next lexicon entry from the given lexicon block file, starting from offset it will read the first 60
     * bytes, then if resetOffset is true, it will reset the offset to the value present ate the beginning, otherwise it
     * will keep the cursor as it is after the read of the entry.
     * @param randomAccessFileLexicon RandomAccessFile of the lexicon block file
     * @param offset offset starting from where to read the lexicon entry
     */
    public static TermInfo readNextTermInfo(RandomAccessFile randomAccessFileLexicon, int offset) {

        //Array of bytes in which put the term
        byte[] termBytes = new byte[TermInfo.TERM_LENGTH];

        //String containing the term
        String term;

        //TermInfo containing the term information to be returned
        TermInfo termInfo;

        try {
            //Set the file pointer to the start of the lexicon entry
            randomAccessFileLexicon.seek(offset);

            //Read the first 48 containing the term
            randomAccessFileLexicon.readFully(termBytes, 0, TermInfo.TERM_LENGTH);

            //Convert the bytes to a string and trim it
            term = new String(termBytes, Charset.defaultCharset()).trim();

            //Instantiate the TermInfo object reading the next 3 integers from the file
            termInfo = new TermInfo(term, randomAccessFileLexicon.readLong(), randomAccessFileLexicon.readLong(), randomAccessFileLexicon.readInt());

            return termInfo;

        } catch (IOException e) {
            //System.err.println("[ReadNextTermInfo] EOF reached while reading the next lexicon entry");
            return null;
        }
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

        //For each block check if it has reached the end of the file
        for(int i = 0; i < numberOfBlocks; i++) {
            if(!endOfBlocks[i])
                //At least one file has not reached the end of the file
                return false;
        }
        //All the files have reached the end of the file
        return true;
    }

    /**
     * Delete the partial block of lexicon and inverted index
     * @param numberOfBlocks number of partial blocks
     * @return true if all the files are successfully deleted, false otherwise
     */
    private static boolean deleteBlocks(int numberOfBlocks) {
        File file;
        for (int i = 0; i < numberOfBlocks; i++) {
            file = new File(INVERTED_INDEX_DOC_IDS_BLOCK_PATH+(i+1)+".txt");
            if(!file.delete())
                return false;
            file = new File(INVERTED_INDEX_FREQUENCIES_BLOCK_PATH+(i+1)+".txt");
            if(!file.delete())
                return false;
            file = new File(LEXICON_BLOCK_PATH+(i+1)+".txt");
            if(!file.delete())
                return false;
        }
        return true;
    }
    public static void main(String[] args){
        merge(true);
    }
}
