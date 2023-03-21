package it.unipi.mircv;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
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
        /*ArrayList<Posting> pl = new ArrayList<>();
        pl.add(new Posting(1,4));
        pl.add(new Posting(2,3));
        pl.add(new Posting(3,12));

        invertedIndex.put("term1", pl);

        pl = new ArrayList<>();
        pl.add(new Posting(1,3));
        pl.add(new Posting(2,1));
        pl.add(new Posting(3,6));

        invertedIndex.put("term2", pl);

        writeInvertedIndexToFile("src/main/resources/files/docids.txt",
                "src/main/resources/files/freqs.txt");*/
        readInvertedIndex();
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

    private static void readInvertedIndex(){
        try (RandomAccessFile randomAccessFile = new RandomAccessFile("src/main/resources/files/freqs.txt", "rw");){

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
