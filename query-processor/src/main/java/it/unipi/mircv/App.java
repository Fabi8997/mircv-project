package it.unipi.mircv;

import it.unipi.mircv.beans.DocumentIndex;
import it.unipi.mircv.beans.Lexicon;
import it.unipi.mircv.beans.PostingList;

public class App 
{
    public static void main( String[] args )
    {
        /*DocumentIndex documentIndex = new DocumentIndex();
        documentIndex.loadDocumentIndex();
        System.out.println(documentIndex.size());*/

        Lexicon lexicon = new Lexicon();
        lexicon.loadLexicon();
        System.out.println(lexicon.size());

        System.out.println(lexicon.get("artificial"));
        System.out.println(lexicon.get("dog"));
        System.out.println(lexicon.get("sport"));

        PostingList postingList = new PostingList();
        postingList.openList(lexicon.get("artificial"));

        System.out.println(postingList);

        postingList = new PostingList();
        postingList.openList(lexicon.get("dog"));

        System.out.println(postingList);

        postingList = new PostingList();
        postingList.openList(lexicon.get("sport"));

        System.out.println(postingList);


        String query = "Artificial intelligence news";
        // TODO: 04/04/2023 parse the query

        // TODO: 04/04/2023 load the postings

        //todo score with daat

        //todo results
    }


}
