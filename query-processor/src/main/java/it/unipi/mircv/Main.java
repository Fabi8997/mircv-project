package it.unipi.mircv;

import it.unipi.mircv.beans.*;
import it.unipi.mircv.parser.Parser;
import it.unipi.mircv.scoring.Score;

import java.util.ArrayList;
import java.util.Scanner;

public class Main
{
    private static Lexicon lexicon = new Lexicon();

    //Flag to indicate if the scoring function is bm25 (true) or TFIDF (false)
    private static boolean bm25scoring = false;

    //Flag to indicate if the queryType is disjunctive (true) or disjunctive (false)
    private static boolean queryType = true;


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

        Configuration configuration = new Configuration();

        //If no configuration is found, then no inverted index is present. The program exits.
        if(!configuration.loadConfiguration())
            return;

        //Flag to indicate if the stopwords removal and stemming are enabled, this must be retrieved from the configuration
        boolean stopwordsRemovalAndStemming = configuration.getStemmingAndStopwordsRemoval();
        
        //Set the initial parameters for the query processor
        setQueryProcessorParameters();

        //Wait for a new command, the while is used to prevent malformed inputs
        //This must be modified in order to have also the possibility to change the query parameters
        while (true) {

            //Get a command from the command line
            int command = getCommand();

            //Check the command
            if(command == 0) { //New query command

                //Read the next query
                String query = getQuery();

                //Parse the query
                String[] queryTerms = parseQuery(query, stopwordsRemovalAndStemming);

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

                // TODO: 17/04/2023 Pass query type argument 
                //Score the collection
                System.out.println(Score.scoreCollection(postingLists,documentIndex, bm25scoring, queryType));

            } else if(command == 1) { //Change settings command

                //Request the new query processor settings then change it
                changeSettings();
                System.out.println("Settings changed!");

            } else if (command == 2) { //Exit command

                System.out.println("See you next query!");
                return;
            }
            //DEFAULT BEHAVIOUR: start the loop again
        }

    }


    /**
     * Method used to take a query in input from the user.
     * @return The input query.
     */
    private static String getQuery(){
        
        //Scanner to read from the standard input stream
        Scanner scanner = new Scanner(System.in);

        System.out.println("Enter a query:");
        
        //Read the query, the -1 at the beginning is used to signal that the string is a query, used during the parsing
        return "-1\t" + scanner.nextLine();
    }


    /**
     * Method used to get the command from the user.
     * @return 0 if the command is to enter a new query, 1 to change the settings, exit to stop the program.
     */
    private static int getCommand(){
        do {

            //Scanner to read from the standard input stream
            Scanner scanner = new Scanner(System.in);

            System.out.println(
                    "0 -> Enter a query\n" +
                    "1 -> Change settings\n" +
                    "2 -> Exit");

            String result;

            if(scanner.hasNext()) {
                result = scanner.nextLine();
                switch (result) {
                    case "0":
                        return 0;
                    case "1":
                        return 1;
                    case "2":
                        return 2;
                }
            }

            System.out.println("Input not valid, enter one of the following commands: ");
        } while (true);
    }

    /**
     * Method used to change the settings of the query processor, it can be used to change the scoring function.
     */
    private static void changeSettings(){
        setQueryProcessorParameters();
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

    private static void setQueryProcessorParameters(){
        Scanner scanner = new Scanner(System.in);
        System.out.println("\nSet the query processor parameters:");
        System.out.println("Scoring function:\n0 -> TFIDF\n1 -> BM25");

        //If 0 => bm25scoring is false, otherwise is true, so we'll use the bm25 scoring function
        bm25scoring = !scanner.nextLine().equals("0");

        System.out.println("Query type:\n0 -> Disjunctive\n1 -> Conjunctive");

        //If 0 => disjunctive, 1 => conjunction, queryType is true with disjunctive and false with conjunctive queries
        queryType = scanner.nextLine().equals("0");

    }



}
