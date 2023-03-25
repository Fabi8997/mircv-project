package it.unipi.mircv.beans;

import it.unipi.mircv.utils.Utils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class ParsedDocument {

    int docId;

    int documentLength;

    String docNo;
    String[] terms;
    
    
    public ParsedDocument(String docNo, String[] terms){
        this.docNo = docNo;
        this.terms = terms;
        documentLength = terms.length;
    }

    public ParsedDocument(int docId, String[] terms, String docNo) {
        this.docId = docId;
        this.documentLength = terms.length;
        this.terms = terms;
        this.docNo = docNo;
    }

    public void setDocId(int docId) {
        this.docId = docId;
    }

    @Override
    public String toString() {
        return "ParsedDocument{" +
                "docId=" + docId +
                ", documentLength=" + documentLength +
                ", docNo='" + docNo + '\'' +
                ", terms=" + Arrays.toString(terms) +
                '}';
    }


    public void writeToDisk(RandomAccessFile randomAccessFile){

        //Fill with whitespaces to keep the length standard
        String tmp = Utils.leftpad(docNo, 48);

        //long begin = System.nanoTime();

        byte[] docId = ByteBuffer.allocate(4).putInt(this.docId).array();
        byte[] docNo = ByteBuffer.allocate(48).put(tmp.getBytes()).array();
        byte[] docLen = ByteBuffer.allocate(4).putInt(this.documentLength).array();

        try {
            randomAccessFile.write(docId);
            randomAccessFile.write(docNo);
            randomAccessFile.write(docLen);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        //System.out.println("[DEBUG: PARSING DOCUMENT] " + (System.nanoTime() - begin)/100000 + "ns");

    }

    public int getDocId() {
        return docId;
    }

    public String[] getTerms() {
        return terms;
    }

}
