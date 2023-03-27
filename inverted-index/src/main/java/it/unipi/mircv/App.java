package it.unipi.mircv;

import it.unipi.mircv.beans.ParsedDocument;
import it.unipi.mircv.beans.Posting;
import it.unipi.mircv.beans.Statistics;
import it.unipi.mircv.beans.TermInfo;
import it.unipi.mircv.builder.InvertedIndexBuilder;

import java.io.FileNotFoundException;
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

    public static void main( String[] args )
    {
        //createBlocks();

        //merge();

    }


    public static void merge(){
        Statistics statistics = readStatistics();
        System.out.println(statistics);

        RandomAccessFile[] randomAccessFileDocIds = new RandomAccessFile[statistics.getNumberOfBlocks()];
        RandomAccessFile[] randomAccessFilesFrequencies = new RandomAccessFile[statistics.getNumberOfBlocks()];
        RandomAccessFile[] randomAccessFilesLexicon = new RandomAccessFile[statistics.getNumberOfBlocks()];
        int[] offsets = new int[statistics.getNumberOfBlocks()];

        for(int i = 0; i < statistics.getNumberOfBlocks(); i++) {
            offsets[i] = 0;
        }

        try {
            for (int i = 0; i < statistics.getNumberOfBlocks(); i++) {
                randomAccessFileDocIds[i] = new RandomAccessFile("src/main/resources/files/invertedIndexDocIds"+(i+1)+".txt", "r");
                randomAccessFilesFrequencies[i] = new RandomAccessFile("src/main/resources/files/invertedIndexFrequencies"+(i+1)+".txt", "r");
                randomAccessFilesLexicon[i] = new RandomAccessFile("src/main/resources/files/lexiconBlock"+(i+1)+".txt", "r");
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }


        String minTerm = null;
        TermInfo curTerm;
        LinkedList<Integer> blocksWithMinTerm = new LinkedList<>();
        boolean[] endOfBlock = new boolean[statistics.getNumberOfBlocks()];
        for (int i = 0; i < statistics.getNumberOfBlocks(); i++) {
            endOfBlock[i] = false;
        }


        // TODO: 24/03/2023 Implementation of the k-way merge algorithm

        while(!endOfAllFiles(endOfBlock, statistics.getNumberOfBlocks())) {
            System.out.println("ITERATION STARTED");
            for(int i = 0; i < statistics.getNumberOfBlocks(); i++) {

                //Read the current term in the lexicon block
                curTerm = readNextTermInfo(randomAccessFilesLexicon[i],offsets[i],true);

                if(curTerm == null) {
                    endOfBlock[i] = true;
                    continue;
                }

                //If the current term is the lexicographically smaller than the min term, then update the min term.
                if(minTerm == null || curTerm.getTerm().compareTo(minTerm) < 0) {
                    minTerm = curTerm.getTerm();
                    blocksWithMinTerm.clear();
                    blocksWithMinTerm.add(i);
                    //Else if the current term is equal to the min term, then add the current block to the list of blocks with the min term.
                } else if (curTerm.getTerm().compareTo(minTerm) == 0) {
                    blocksWithMinTerm.add(i);
                }
            }
            if(endOfAllFiles(endOfBlock, statistics.getNumberOfBlocks())) {
                System.out.println("END OF ALL FILES");
                break;
            }

            System.out.println("----------- TERM: " + minTerm + " -----------");
            System.out.println(blocksWithMinTerm);
            TermInfo currentTermInfo = null;
            ArrayList<Integer> docIds = new ArrayList<>();
            ArrayList<Integer> frequencies = new ArrayList<>();
            for (Integer integer : blocksWithMinTerm) {
                System.out.println("Block " + integer + ":");

                currentTermInfo = readNextTermInfo(randomAccessFilesLexicon[integer], offsets[integer], false);
                if(currentTermInfo == null) {
                    continue;
                }
                offsets[integer] += 60;
                System.out.println("DOCID-"+integer+": " + readPostingListDocIds(randomAccessFileDocIds[integer], currentTermInfo.getOffsetDocId(), currentTermInfo.getPostingListLength()));
                docIds.addAll(readPostingListDocIds(randomAccessFileDocIds[integer], currentTermInfo.getOffsetDocId(), currentTermInfo.getPostingListLength()));
                System.out.println("FREQ-"+integer+": " + readPostingListFrequencies(randomAccessFilesFrequencies[integer], currentTermInfo.getOffsetFrequency(), currentTermInfo.getPostingListLength()));
                frequencies.addAll(readPostingListFrequencies(randomAccessFilesFrequencies[integer], currentTermInfo.getOffsetFrequency(), currentTermInfo.getPostingListLength()));
            }
            // TODO: 25/03/2023 Instead of printing, write to a file.
            System.out.println("DocIds-merged:" + docIds);
            System.out.println("Frequencies-merged" + frequencies);

            //System.out.println(Arrays.toString(offsets));

            System.out.println("----------------------");
            minTerm = null;
            blocksWithMinTerm.clear();
        }
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
        invertedIndexBuilder.insertDocument(new ParsedDocument(2,str2.toLowerCase().split(" "),"2"));
        invertedIndexBuilder.insertDocument(new ParsedDocument(3,str3.toLowerCase().split(" "),"3"));

        invertedIndexBuilder.sortLexicon();
        invertedIndexBuilder.sortInvertedIndex();
        writeToFiles(invertedIndexBuilder, 1);

        invertedIndexBuilder.insertDocument(new ParsedDocument(4,str4.toLowerCase().split(" "),"4"));
        invertedIndexBuilder.insertDocument(new ParsedDocument(5,str5.toLowerCase().split(" "),"5"));

        invertedIndexBuilder.sortLexicon();
        invertedIndexBuilder.sortInvertedIndex();
        writeToFiles(invertedIndexBuilder, 2);

        invertedIndexBuilder.insertDocument(new ParsedDocument(6,str6.toLowerCase().split(" "),"6"));

        invertedIndexBuilder.sortLexicon();
        invertedIndexBuilder.sortInvertedIndex();
        writeToFiles(invertedIndexBuilder, 3);
    }

    // TODO: 25/03/2023 DONE
    private static boolean endOfAllFiles(boolean[] endOfBlocks, int numberOfBlocks) {

        for(int i = 0; i < numberOfBlocks; i++) {
            if(!endOfBlocks[i])
                return false;
        }
        return true;
    }

    // TODO: 24/03/2023 DONE
    public static TermInfo readNextTermInfo(RandomAccessFile randomAccessFileLexicon, int offset, boolean resetOffset) {

        byte[] b;
        String term;
        b = new byte[48];
        TermInfo termInfo;

        try {
            //System.out.println("[readNextTermInfo] Reading term info from file");
            //System.out.println("[readNextTermInfo] randomAccessFileLexicon.length: " + randomAccessFileLexicon.length());
            //System.out.println("[readNextTermInfo] Offset: " + offset);
            randomAccessFileLexicon.seek(offset);
            randomAccessFileLexicon.readFully(b, 0, 48);
            term = new String(b, Charset.defaultCharset()).trim();
            termInfo = new TermInfo(term, randomAccessFileLexicon.readInt(), randomAccessFileLexicon.readInt(), randomAccessFileLexicon.readInt());
            if(resetOffset)
                randomAccessFileLexicon.seek(offset);
            return termInfo;
        } catch (IOException e) {
            return null;
        }
    }

    // TODO: 24/03/2023 DONE
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

        System.out.println("Inverted index: \n"+invertedIndexBuilder.getInvertedIndex());
        System.out.println("Lexicon: \n"+invertedIndexBuilder.getLexicon());

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

    //TODO: 24/03/2023 DONE
    private static ArrayList<Integer> readPostingListDocIds(RandomAccessFile randomAccessFileDocIds, int offset, int length) {

        byte[] b = new byte[4];
        long rafLength = 0;

        ArrayList<Integer> list = new ArrayList<>();

        try {
            randomAccessFileDocIds.seek(offset);
            rafLength = randomAccessFileDocIds.length();
        } catch (IOException e) {
            System.err.println("[ReadPostingListDocIds] Exception during seek");
            throw new RuntimeException(e);
        }
        for(int i = 0; i < length; i ++) {
            try {
                list.add(randomAccessFileDocIds.readInt());
            } catch (IOException e) {
                /*System.err.println("[ReadPostingListDocIds] Exception during read");
                System.err.println("[ReadPostingListDocIds] randomAccessFileDocIds.length: " + rafLength);
                System.err.println("[ReadPostingListDocIds] offset: " + offset);
                System.err.println("[ReadPostingListDocIds] length: " + length);*/
                throw new RuntimeException(e);
            }
        }
        return list;
    }

    //TODO: 24/03/2023 DONE
    private static ArrayList<Integer> readPostingListFrequencies(RandomAccessFile randomAccessFileFrequencies, int offset, int length) {
        byte[] b = new byte[4];

        ArrayList<Integer> list = new ArrayList<>();

        try {
            randomAccessFileFrequencies.seek(offset);
        } catch (IOException e) {
            System.err.println("[ReadPostingListFrequencies] Exception during seek");
            throw new RuntimeException(e);
        }
        for(int i = 0; i < length; i ++) {
            try {
                list.add(randomAccessFileFrequencies.readInt());
            } catch (IOException e) {
                System.err.println("[ReadPostingListFrequencies] Exception during read");
                throw new RuntimeException(e);
            }
        }
        return list;
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
