package it.unipi.mircv;

import sun.nio.cs.UTF_8;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        write();

        read();
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
        hashMap.put("Prova", new TermInfo(1, 0));
        hashMap.put("Secondo", new TermInfo(2, 0));

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
