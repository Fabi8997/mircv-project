package it.unipi.mircv;

import it.unipi.mircv.beans.*;
import it.unipi.mircv.builder.InvertedIndexBuilder;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hello world!
 *
 */
public class App 
{
    static HashMap<String, ArrayList<Posting>> invertedIndex = new HashMap<>();

    public static void main( String[] args ) throws IOException {
        createBlocks();

        //merge();

        /*Lexicon lexicon = new Lexicon();
        lexicon.loadLexicon();
        System.out.println(lexicon.get("to"));

        RandomAccessFile raf = new RandomAccessFile("Files/docids.txt","r");
        System.out.println(raf.length());

        byte[] bytes = readBytes(raf,135,11);
        System.out.println(Compressor.variableByteDecode(bytes));

        /*System.out.println(readPostingListDocIds(raf,472,4));*/

        /*DocumentIndex documentIndex = new DocumentIndex();
        documentIndex.loadDocumentIndex();
        System.out.println(documentIndex.size());
        System.out.println(documentIndex.get(1L));*/
    }


    private static void createBlocks(){
        InvertedIndexBuilder invertedIndexBuilder = new InvertedIndexBuilder();

        String str1= "A bijection from the set X to the set Y has an inverse function from Y to X";
        String str2 = "In mathematics a bijection also known as a bijective function one to one correspondence or invertible function";
        String str3 = "here are no unpaired elements between the two sets";
        String str4 = "If X and Y are finite sets then the existence of a bijection means they have the same number of elements";
        String str5 = "A bijective function from a set to itself is also called a permutation";
        String str6 = "unpaired to between to permutation set";

        invertedIndexBuilder.insertDocument(new ParsedDocument(1,str1.toLowerCase().split(" "),"1"));
        invertedIndexBuilder.insertDocument(new ParsedDocument(128,str2.toLowerCase().split(" "),"2"));
        invertedIndexBuilder.insertDocument(new ParsedDocument(258,str3.toLowerCase().split(" "),"3"));

        invertedIndexBuilder.sortLexicon();
        invertedIndexBuilder.sortInvertedIndex();
        writeToFiles(invertedIndexBuilder, 1);

        invertedIndexBuilder.insertDocument(new ParsedDocument(12000,str4.toLowerCase().split(" "),"4"));
        invertedIndexBuilder.insertDocument(new ParsedDocument(2121990,str5.toLowerCase().split(" "),"5"));

        invertedIndexBuilder.sortLexicon();
        invertedIndexBuilder.sortInvertedIndex();
        writeToFiles(invertedIndexBuilder, 2);

        invertedIndexBuilder.insertDocument(new ParsedDocument(3000000,str6.toLowerCase().split(" "),"6"));

        invertedIndexBuilder.sortLexicon();
        invertedIndexBuilder.sortInvertedIndex();
        writeToFiles(invertedIndexBuilder, 3);
    }

    private static void writeToFiles(InvertedIndexBuilder invertedIndexBuilder, int blockNumber){

        //Write the inverted index's files into the block's files
        invertedIndexBuilder.writeInvertedIndexToFile(
                "src/main/resources/tmp/invertedIndexDocIds"+blockNumber+".txt",
                "src/main/resources/tmp/invertedIndexFrequencies"+blockNumber+".txt");

        //Write the block's lexicon into the given file
        invertedIndexBuilder.writeLexiconToFile("src/main/resources/tmp/lexiconBlock"+blockNumber+".txt");

        System.out.println("Block "+blockNumber+" written");

        System.out.println("Inverted index: \n"+invertedIndexBuilder.getInvertedIndex());
        System.out.println("Lexicon: \n"+invertedIndexBuilder.getLexicon());

        //Clear the inverted index and lexicon data structure and call the garbage collector
        invertedIndexBuilder.clear();
    }

}
