package it.unipi.mircv;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
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

    static void merge() {
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

    private static Statistics readStatistics(){
        return new Statistics();
    }

}
