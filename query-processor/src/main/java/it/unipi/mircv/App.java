package it.unipi.mircv;

import it.unipi.mircv.beans.DocumentIndex;
import it.unipi.mircv.beans.Lexicon;
import it.unipi.mircv.beans.PostingList;
import it.unipi.mircv.beans.Statistics;

import java.util.Scanner;

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

        System.out.println(lexicon.get("ball"));
        System.out.println(lexicon.get("dog"));
        System.out.println(lexicon.get("sport"));

        PostingList postingList = new PostingList();
        postingList.openList(lexicon.get("ball"));

        System.out.println("Ball: " + postingList);

        postingList = new PostingList();
        postingList.openList(lexicon.get("dog"));

        System.out.println("Dog: " + postingList);

        postingList = new PostingList();
        postingList.openList(lexicon.get("sport"));

        System.out.println("Sport" + postingList);

        // TODO: 04/04/2023 parse the query
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter a String :");
        String c = scanner.nextLine();

        // TODO: 04/04/2023 load the postings

        //todo score with daat

        //todo results
    }


}
