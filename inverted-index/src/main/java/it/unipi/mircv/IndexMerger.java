package it.unipi.mircv;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;


public class IndexMerger {

    final String INVERTED_INDEX_DOC_IDS_BLOCK_PATH = "src/main/resources/files/invertedIndexDocIds";

    final String INVERTED_INDEX_FREQUENCIES_BLOCK_PATH = "src/main/resources/files/invertedIndexFrequencies";

    final String LEXICON_BLOCK_PATH = "src/main/resources/files/lexiconBlock";

    final String LEXICON_PATH = "src/main/resources/files/lexicon.txt";

    final String INVERTED_INDEX_PATH = "src/main/resources/files/invertedIndex.txt";

    HashMap<String, Integer> lexicon;

    HashMap<String, Integer> lexiconBlock;

    LinkedList<LinkedList<Integer>> invertedIndexDocIdsBlock;

    LinkedList<LinkedList<Integer>> invertedIndexFrequenciesBlock;

    public static void merge() {
        Statistics statistics = readStatistics();
        System.out.println(statistics);

        RandomAccessFile[] randomAccessFileDocIds = new RandomAccessFile[statistics.numberOfBlocks];
        RandomAccessFile[] randomAccessFilesFrequencies = new RandomAccessFile[statistics.numberOfBlocks];
        RandomAccessFile[] randomAccessFilesLexicon = new RandomAccessFile[statistics.numberOfBlocks];

        try {
            for (int i = 0; i < statistics.numberOfBlocks; i++) {
                randomAccessFileDocIds[i] = new RandomAccessFile("src/main/resources/files/invertedIndexDocIds"+i+".txt", "r");
                randomAccessFilesFrequencies[i] = new RandomAccessFile("src/main/resources/files/invertedIndexFrequencies"+i+".txt", "r");
                randomAccessFilesLexicon[i] = new RandomAccessFile("src/main/resources/files/lexiconBlock"+i+".txt", "r");
            }

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        mergeBlocks(randomAccessFileDocIds, randomAccessFilesFrequencies, randomAccessFilesLexicon);

    }

    private static void mergeBlocks(RandomAccessFile[] randomAccessFileDocIds, RandomAccessFile[] randomAccessFilesFrequencies, RandomAccessFile[] randomAccessFilesLexicon) {

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
            //Read the first 48 containing the term
            randomAccessFileLexicon.readFully(termBytes, offset, 48);

            //Convert the bytes to a string and trim it
            term = new String(termBytes, Charset.defaultCharset()).trim();

            //Instantiate the TermInfo object reading the next 3 integers from the file
            termInfo = new TermInfo(term, randomAccessFileLexicon.readInt(), randomAccessFileLexicon.readInt(), randomAccessFileLexicon.readInt());

            if(resetOffset)
                //Reset the offset to the value present before the read of the entry
                randomAccessFileLexicon.seek(offset);

            return termInfo;

        } catch (IOException e) {
            System.err.println("[ReadNextTermInfo] Error reading the next term from the lexicon block file");
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
}
