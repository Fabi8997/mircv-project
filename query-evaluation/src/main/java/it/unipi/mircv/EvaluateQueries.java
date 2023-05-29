package it.unipi.mircv;

import it.unipi.mircv.beans.*;
import it.unipi.mircv.parser.Parser;
import it.unipi.mircv.scoring.Score;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Main class for the query evaluation module, provide the methods to load a batch of queries used to generate a file
 * containing the results of the queries in a format that can be used to compute the metrics using the trec_eval tool.
 */
public class EvaluateQueries{

    //Path to the input file containing the queries
    static final String QUERY_PATH = "Dataset/queries.tsv.gz";

    //Path to the output file containing the results of the queries
    static final String RESULTS_PATH = "Files/queries_results.txt";

    //bm25scoring flag indicating if the scoring is bm25 (true) or tfidf (false)
    static final Boolean bm25scoring = true;

    //flag indicating if the query is disjunctive (true) or disjunctive (false)
    static final Boolean queryType = true;

    
    public static void main( String[] args )
    {

        Configuration configuration = new Configuration();

        //If no configuration is found, then no inverted index is present. The program exits.
        if(!configuration.loadConfiguration())
            return;

        System.out.println("[QUERY PROCESSOR] Building inverted index configuration:");
        System.out.println(configuration);

        System.out.println("[QUERY PROCESSOR] Loading the lexicon in memory...");
        Lexicon lexicon = new Lexicon();
        lexicon.loadLexicon();

        System.out.println("[QUERY PROCESSOR] Loading the document index in memory...");
        DocumentIndex documentIndex = new DocumentIndex();
        documentIndex.loadDocumentIndex();

        evaluateQueries(getQueries(), configuration, documentIndex, lexicon);


    }

    /**
     * Read from a file a list of queries in the format of qid\tquery and return an array of tuple containing the
     * qid and the query: (qid, query)
     * @return an array of tuple containing the qid and the query: (qid, query)
     */
    private static ArrayList<Tuple<Long, String>> getQueries(){

        //Path of the collection to be read
        File file = new File(QUERY_PATH);

        //Try to open the collection provided
        try (FileInputStream fileInputStream = new FileInputStream(file)){

                //Read the uncompressed tar file specifying UTF-8 as encoding
                InputStreamReader inputStreamReader = new InputStreamReader(new GzipCompressorInputStream(fileInputStream), StandardCharsets.UTF_8);

                //Create a BufferedReader in order to access one line of the file at a time
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                //Variable to keep the current line read from the buffer
                String line;

                //Array list for the results
                ArrayList<Tuple<Long, String>> results = new ArrayList<>();

                //Iterate over the lines
                while ((line = bufferedReader.readLine()) != null ) {

                    //Split the line qid\tquery in qid query
                    String[] split = line.split("\t");

                    //Add it to the results array
                    if(split[0] != null && split[1] != null) {
                        results.add(new Tuple<>(Long.parseLong(split[0]), split[1]));
                    }
                }

                return results;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Performs all the queries in the array of queries, using the configuration parameters passed, for the scoring it
     * requires the document index and the lexicon.
     * @param queries array of tuples containing the query id and the query string (queryId, query)
     * @param configuration configuration used during the creation of the previous inverted index
     * @param documentIndex document index containing the document info
     * @param lexicon lexicon containing the terms information
     */
    private static void evaluateQueries(ArrayList<Tuple<Long,String>> queries, Configuration configuration, DocumentIndex documentIndex, Lexicon lexicon){

        //Object used to build the lexicon line into a string
        StringBuilder stringBuilder;

        //Buffered writer used to format the output
        BufferedWriter bufferedWriter;

        try {

            bufferedWriter = new BufferedWriter(new FileWriter(RESULTS_PATH,false));

            double completionTimeTot = 0.0;
            int numberOfQueries = 0;

            for( Tuple<Long,String> tuple : queries ){

                //Read the next query, add -1 to indicate that it is a query
                String query = "-1\t" + tuple.getSecond();

                //Parse the query
                String[] queryTerms = parseQuery(query, lexicon, configuration.getStemmingAndStopwordsRemoval());

                System.out.println("Query: " + query + "\t" + "Terms: " + Arrays.toString(queryTerms));

                //If the query string is equal to null it means that the query contains all stopwords or all the terms
                // were written in a bad way or not present in the lexicon.
                if(queryTerms == null || queryTerms.length == 0) {
                    System.out.println("You're query is too vague, try to reformulate it.");
                    continue;
                }

                //Remove the duplicates
                queryTerms = Arrays.stream(queryTerms).distinct().toArray(String[]::new);

                System.out.println("Query: " + query + "\t" + "Terms: " + Arrays.toString(queryTerms));

                //Load the posting list of the terms of the query
                PostingList[] postingLists = new PostingList[queryTerms.length];

                //For each term in the query terms array
                for (int i = 0; i < queryTerms.length; i++) {

                    //Instantiate the posting for the i-th query term
                    postingLists[i] = new PostingList();

                    //Load in memory the posting list of the i-th query term
                    postingLists[i].openList(lexicon.get(queryTerms[i]));

                }

                //Array to hold the results of the query
                ArrayList<Tuple<Long, Double>> result;

                //Score the collection

                //Retrieve the time at the beginning of the computation
                long begin = System.currentTimeMillis();

                if(queryType){
                    result = Score.scoreCollectionDisjunctive(postingLists,documentIndex, bm25scoring, false);
                }else {
                    result = Score.scoreCollectionConjunctive(postingLists,documentIndex, bm25scoring, false);
                }

                completionTimeTot += (System.currentTimeMillis() - begin);
                numberOfQueries++;

                //Write the results in a format valid for the TREC_EVAL tool
                for(int i = 0; i < result.size(); i++){

                    //New string builder for the current result
                    stringBuilder = new StringBuilder();

                    //build the string
                    stringBuilder
                            .append(tuple.getFirst()).append(" ")
                            .append("q0 ")
                            .append(documentIndex.get(result.get(i).getFirst()).getDocNo()).append(" ")
                            .append(i+1).append(" ")
                            .append(result.get(i).getSecond()).append(" ")
                            .append("runid1").append("\n");

                    //Write the string in the file
                    bufferedWriter.write(stringBuilder.toString());

                }

                //Close the posting lists
                for (PostingList postingList : postingLists) {
                    postingList.closeList();
                }

            }

            System.out.println("Average completion time: " + completionTimeTot/numberOfQueries + "ms");
            System.out.println("Number of queries: " + numberOfQueries);

            //Close the writer
            bufferedWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Parses the query and returns the list of terms containing the query, the parsing process must be the same as the
     * one used during the indexing phase.
     * @param query the query string to parse
     * @param stopwordsRemovalAndStemming if true remove the stopwords and applies the stemming procedure.
     * @return the array of terms after the parsing of the query
     */
    public static String[] parseQuery(String query, Lexicon lexicon ,boolean stopwordsRemovalAndStemming) {

        //Array of terms to build the result
        ArrayList<String> results = new ArrayList<>();

        System.out.println(query);

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


}
