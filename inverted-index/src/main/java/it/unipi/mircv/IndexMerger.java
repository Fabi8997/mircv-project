package it.unipi.mircv;

import java.util.HashMap;
import java.util.LinkedList;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;


// TODO: 03/11/2022 Read the blocks in a taat or daat way in order to form the posting and the overall lexicon
public class IndexMerger {

    final int NUMBER_OF_BLOCKS;

    final String INVERTED_INDEX_DOC_IDS_BLOCK_PATH = "src/main/resources/files/invertedIndexDocIds";

    final String INVERTED_INDEX_FREQUENCIES_BLOCK_PATH = "src/main/resources/files/invertedIndexFrequencies";

    final String LEXICON_BLOCK_PATH = "src/main/resources/files/lexiconBlock";

    final String LEXICON_PATH = "src/main/resources/files/lexicon.txt";

    final String INVERTED_INDEX_PATH = "src/main/resources/files/invertedIndex.txt";

    HashMap<String, Integer> lexicon;

    HashMap<String, Integer> lexiconBlock;

    LinkedList<LinkedList<Integer>> invertedIndexDocIdsBlock;

    LinkedList<LinkedList<Integer>> invertedIndexFrequenciesBlock;

    IndexMerger(){
        NUMBER_OF_BLOCKS = new Statistics().numberOfBlocks;
        lexicon = new HashMap<>();
        lexiconBlock = new HashMap<>();
        invertedIndexDocIdsBlock = new LinkedList<>();
        invertedIndexFrequenciesBlock = new LinkedList<>();
    }

    public void mergeBlocks(){

        for(int i = 1; i <= NUMBER_OF_BLOCKS; i++){
            loadLexiconBlock(i);

        }

    }

    private void loadLexiconBlock(int block){
        try
        {
            File file=new File(LEXICON_BLOCK_PATH + block + ".txt");    //creates a new file instance
            FileReader fr=new FileReader(file);   //reads the file
            BufferedReader br=new BufferedReader(fr);  //creates a buffering character input stream
            String line;
            while((line=br.readLine())!=null)
            {
                String[] words = line.split("\t");   //line feed
                lexiconBlock.put(words[0], Integer.parseInt(words[1]) );
            }
            fr.close();    //closes the stream and release the resources
            System.out.println("Contents of File: " + lexiconBlock.toString());

        }
        catch(IOException e)
        {
            e.printStackTrace();
        }
    }

    public static void main(String[] args){
        IndexMerger indexMerger= new IndexMerger();
        indexMerger.mergeBlocks();
    }
}
