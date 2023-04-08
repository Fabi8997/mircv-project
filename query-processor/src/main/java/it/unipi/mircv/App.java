package it.unipi.mircv;

import it.unipi.mircv.beans.*;
import it.unipi.mircv.parser.Parser;
import it.unipi.mircv.scoring.Score;

import java.util.ArrayList;
import java.util.Scanner;

public class App 
{
    private static Lexicon lexicon = new Lexicon();

    private static boolean bm25scoring = false;

    public static void main( String[] args )
    {
        System.out.println("[QUERY PROCESSOR] Loading the lexicon in memory...");

        lexicon = new Lexicon();
        lexicon.loadLexicon();
        System.out.println("[QUERY PROCESSOR] Lexicon size: " + lexicon.size());

        System.out.println("[QUERY PROCESSOR] Loading the document index in memory...");

        DocumentIndex documentIndex = new DocumentIndex();
        documentIndex.loadDocumentIndex();
        System.out.println("[QUERY PROCESSOR] Document index size: " + documentIndex.size());

        System.out.println("[QUERY PROCESSOR] Data structures loaded in memory.");

        // TODO: 08/04/2023 Retrieve the configuration of the indexer

        // TODO: 08/04/2023 Select the initial settings -> bm25 or TFIDF and so on...

        //Wait for a new command, the while is used to prevent malformed inputs
        //This must be modified in order to have also the possibility to change the query parameters
        while (true) {

            // TODO: 08/04/2023 Get command!
            //if malformed input command -> print malformed -> continue

            //Read the next query
            String query = getQuery();

            //If the query string is equal to null it means that the exit command was given.
            if(query == null){
                System.out.println("See you next query!");
                return;
            }

            // TODO: 08/04/2023 Retrieve the configuration of the indexer and get the stopwords condition 
            //Parse the query
            String[] queryTerms = parseQuery(query, true);

            //If the query string is equal to null it means that the query contains all stopwords or all the terms
            // were written in a bad way or not present in the lexicon.
            if(queryTerms == null){
                System.out.println("You're query is too vague, try to reformulate it.");
                continue;
            }

            //Load the posting list of the terms of the query
            PostingList[] postingLists = new PostingList[queryTerms.length];

            //For each term in the query terms array
            for (int i = 0; i < queryTerms.length; i++) {

                //Instantiate the posting for the i-th query term
                postingLists[i] = new PostingList();

                //Load in memory the posting list of the i-th query term
                postingLists[i].openList(lexicon.get(queryTerms[i]));

                //Debug
                //System.out.println(queryTerms[i] + ": " + postingLists[i].size());
            }

            //Score the collection
            System.out.println(Score.scoreCollection(postingLists,documentIndex, bm25scoring));
        }
        
    }


    /**
     * Method used to take a query in input from the user or to end the application.
     * @return The input query.
     */
    private static String getQuery(){
        
        //Scanner to read from the standard input stream
        Scanner scanner = new Scanner(System.in);

        do {
            System.out.println(
                    "0 -> enter a query\n" +
                            "1 -> exit");

            //Check the input
            if (scanner.hasNext() && scanner.nextLine().equals("1")) {
                return null;
            }
        } while (!scanner.hasNext() || !scanner.nextLine().equals("0"));
        
        System.out.println("Enter a query:");
        
        //Read the query, the -1 at the beginning is used to signal that the string is a query, used during the parsing
        return "-1\t" + scanner.nextLine();
    }

    /**
     * Parses the query and returns the list of terms containing the query, the parsing process must be the same as the
     * one used during the indexing phase.
     * @param query the query string to parse
     * @param stopwordsRemovalAndStemming if true remove the stopwords and applies the stemming procedure.
     * @return the list of terms after the parsing of the query
     */
    private static String[] parseQuery(String query, boolean stopwordsRemovalAndStemming) {

        //Array of terms to build the result
        ArrayList<String> results = new ArrayList<>();

        //Parse the query using the same configuration of the indexer
        ParsedDocument parsedDocument = Parser.processDocument(query, stopwordsRemovalAndStemming);
        
        //If no terms are returned by the parser then return null
        if(parsedDocument == null){
            return null;
        }

        //Remove the query terms that are not present in the lexicon
        for(String term : parsedDocument.getTerms()){
            if(lexicon.get(term) != null){
                results.add(term);
            }
        }

        //Return an array of String containing the results of the parsing process
        return results.toArray(new String[0]);
    }

    // TODO: 08/04/2023 Add a part during the startup to select the parameters: TFIDF OR BM25, DISJUNCTIVE OR CONJUNCTIVE
    private static void setQueryProcessorParameters(){

    }

}
