package it.unipi.mircv;

import it.unipi.mircv.beans.DocumentIndex;
import it.unipi.mircv.beans.Lexicon;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        DocumentIndex documentIndex = new DocumentIndex();
        documentIndex.loadDocumentIndex();
        System.out.println(documentIndex.size());

        Lexicon lexicon = new Lexicon();
        lexicon.loadLexicon();
        System.out.println(lexicon.size());
    }

}
