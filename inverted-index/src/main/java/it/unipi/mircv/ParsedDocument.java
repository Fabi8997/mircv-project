package it.unipi.mircv;

public class ParsedDocument {

    int docId;

    int documentLength;

    String docNo;
    String[] terms;

    public ParsedDocument(int docId, String[] terms){
        this.docId = docId;
        this.terms = terms;
        documentLength = terms.length;
    }

    public void setDocId(int docId) {
        this.docId = docId;
    }
}
