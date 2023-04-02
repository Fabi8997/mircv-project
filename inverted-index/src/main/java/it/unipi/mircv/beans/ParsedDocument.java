package it.unipi.mircv.beans;

import java.util.Arrays;

public class ParsedDocument {

    long docId;

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

    public long getDocId() {
        return docId;
    }

    public String[] getTerms() {
        return terms;
    }

    public int getDocumentLength() {
        return documentLength;
    }

    public String getDocNo() {
        return docNo;
    }
}
