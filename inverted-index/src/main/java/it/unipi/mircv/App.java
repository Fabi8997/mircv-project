package it.unipi.mircv;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hello world!
 *
 */
public class App 
{
    static HashMap<String, ArrayList<Posting>> invertedIndex = new HashMap<>();

    public static void main( String[] args )
    {
        /*InvertedIndexBuilder invertedIndexBuilder = new InvertedIndexBuilder();

        String str1= "A bijection from the set X to the set Y has an inverse function from Y to X";
        String str2 = "In mathematics a bijection also known as a bijective function one to one correspondence or invertible function";
        String str3 = "here are no unpaired elements between the two sets";
        String str4 = "If X and Y are finite sets then the existence of a bijection means they have the same number of elements";
        String str5 = "A bijective function from a set to itself is also called a permutation";

        invertedIndexBuilder.insertDocument(new ParsedDocument(1,str1.split(" "),"1"));
        invertedIndexBuilder.insertDocument(new ParsedDocument(2,str2.split(" "),"2"));
        invertedIndexBuilder.insertDocument(new ParsedDocument(3,str3.split(" "),"3"));

        invertedIndexBuilder.sortLexicon();
        invertedIndexBuilder.sortInvertedIndex();
        writeToFiles(invertedIndexBuilder, 1);

        invertedIndexBuilder.insertDocument(new ParsedDocument(4,str4.split(" "),"4"));
        invertedIndexBuilder.insertDocument(new ParsedDocument(5,str5.split(" "),"5"));

        invertedIndexBuilder.sortLexicon();
        invertedIndexBuilder.sortInvertedIndex();
        writeToFiles(invertedIndexBuilder, 2);*/

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

        // TODO: 24/03/2023 Implementation of the k-way merge algorithm 

    }

    public TermInfo readLexiconFromFile(RandomAccessFile randomAccessFileLexicon, int offset){

        byte[] b;
        String term;
        b = new byte[48];
        TermInfo termInfo;

        try {
            randomAccessFileLexicon.readFully(b, offset, 48);
            term = new String(b, Charset.defaultCharset()).trim();
            termInfo = new TermInfo(term, randomAccessFileLexicon.readInt(), randomAccessFileLexicon.readInt(), randomAccessFileLexicon.readInt());
            randomAccessFileLexicon.seek(offset);
            return termInfo;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Statistics readStatistics(){
        return new Statistics();
    }

    private static void writeToFiles(InvertedIndexBuilder invertedIndexBuilder, int blockNumber){

        //Write the inverted index's files into the block's files
        invertedIndexBuilder.writeInvertedIndexToFile(
                "src/main/resources/files/invertedIndexDocIds"+blockNumber+".txt",
                "src/main/resources/files/invertedIndexFrequencies"+blockNumber+".txt");

        //Write the block's lexicon into the given file
        invertedIndexBuilder.writeLexiconToFile("src/main/resources/files/lexiconBlock"+blockNumber+".txt");

        System.out.println("Block "+blockNumber+" written");

        System.out.println("Inverted index: \n"+invertedIndexBuilder.invertedIndex);
        System.out.println("Lexicon: \n"+invertedIndexBuilder.lexicon);

        //Clear the inverted index and lexicon data structure and call the garbage collector
        invertedIndexBuilder.clear();
    }



    public static void writeInvertedIndexToFile(String outputPathDocIds, String outputPathFrequencies){

        try (RandomAccessFile docIdBlock = new RandomAccessFile(outputPathDocIds, "rw");
             RandomAccessFile frequencyBlock = new RandomAccessFile(outputPathFrequencies, "rw");)
        {

            invertedIndex.forEach((term, postingList) -> {    //for each element of the inverted index
                AtomicInteger pos = new AtomicInteger(1);

                postingList.forEach(posting -> {
                    byte[] postingDocId = ByteBuffer.allocate(4).putInt(posting.getDoc_id()).array();
                    byte[] postingFreq = ByteBuffer.allocate(4).putInt(posting.getFrequency()).array();

                    try {
                        docIdBlock.write(postingDocId);
                        frequencyBlock.write(postingFreq);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    pos.getAndIncrement();
                });
            });
        }catch (IOException e) {
            System.err.println("Exception during file creation of block");
            throw new RuntimeException(e);
        }
    }

    private static void readInvertedIndex(int block){
        try (RandomAccessFile randomAccessFile = new RandomAccessFile("src/main/resources/files/invertedIndexDocIds"+block+".txt", "rw");){

            byte[] b = new byte[4];

            ArrayList<Integer> list = new ArrayList<>();
            long length = randomAccessFile.length()/4;
            System.out.println("LENGTH: " + length);

            for(int i = 0; i < length; i ++){
                list.add(randomAccessFile.readInt());
            }

            System.out.println(list);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void prova(){
        Runtime rt = Runtime.getRuntime();
        long total_mem = rt.totalMemory();
        long free_mem = rt.freeMemory();
        long used_mem = total_mem - free_mem;
        long percentage_used_mem = used_mem/total_mem;
        System.out.println("Amount of used memory: " + percentage_used_mem + "%");
    }

    private static void write(){

        HashMap<String, TermInfo> hashMap = new HashMap<>();
        hashMap.put("Prova", new TermInfo(1, 1, 10));
        hashMap.put("Secondo", new TermInfo(2, 2, 21));

        try (RandomAccessFile randomAccessFile = new RandomAccessFile("src/main/resources/files/provaWrite", "rw");){
            String prova = "abcdedasadsa";
            prova = leftpad(prova, 48);
            System.out.println(prova.length());

            byte[] bytes1 = ByteBuffer.allocate(48).put(prova.getBytes()).array();
            byte[] bytes2 = ByteBuffer.allocate(4).putInt(6).array();
            byte[] bytes3 = ByteBuffer.allocate(4).putInt(1).array();

            randomAccessFile.write(bytes1);
            randomAccessFile.write(bytes2);
            randomAccessFile.write(bytes3);
        }



        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void read(){
        try (RandomAccessFile randomAccessFile = new RandomAccessFile("src/main/resources/files/provaWrite", "rw");){

            byte[] b = new byte[48];
            randomAccessFile.seek(0);
            randomAccessFile.readFully(b, 0, 48);
            String name = new String(b, Charset.defaultCharset()).trim();

            System.out.println(name);
            System.out.println(randomAccessFile.readInt());
            System.out.println(randomAccessFile.readInt());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String leftpad(String text, int length) {
        return String.format("%" + length + "." + length + "s", text);
    }

    private static String rightpad(String text, int length) {
        return String.format("%-" + length + "." + length + "s", text);
    }


}
