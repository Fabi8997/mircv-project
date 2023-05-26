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
 * Hello world!
 *
 */
public class EvaluateQueries{

    static final String QUERY_PATH = "Dataset/queries.tsv.gz";

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

                    String[] split = line.split("\t");

                    if(split[0] != null && split[1] != null) {
                        results.add(new Tuple<>(Long.getLong(split[0]), split[1]));
                    }
                }

                return results;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void evaluateQueries(ArrayList<Tuple<Long,String>> queries, Configuration configuration, DocumentIndex documentIndex, Lexicon lexicon, Boolean bm25scoring, Boolean queryType){

        for( Tuple<Long,String> tuple : queries ){

            //Read the next query
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

            //Load the posting list of the terms of the query
            PostingList[] postingLists = new PostingList[queryTerms.length];

            //For each term in the query terms array
            for (int i = 0; i < queryTerms.length; i++) {

                //Instantiate the posting for the i-th query term
                postingLists[i] = new PostingList();

                //Load in memory the posting list of the i-th query term
                postingLists[i].openList(lexicon.get(queryTerms[i]));

            }

            ArrayList<Tuple<Long, Double>> result;

            //Score the collection
            if(queryType){
                result = Score.scoreCollectionDisjunctive(postingLists,documentIndex, bm25scoring, false);
            }else {
                result = Score.scoreCollectionConjunctive(postingLists,documentIndex, bm25scoring, false);
            }

            // TODO: 26/05/2023 Instead of printing the results, write it to the file 
            //Print the results in a formatted way
            System.out.println("\n#\tDOCNO\t\tSCORE");
            for(int i = 0; i < result.size(); i++){
                System.out.println((i+1) +
                        ")\t" +
                        documentIndex.get(result.get(i).getFirst()).getDocNo() +
                        "\t"+result.get(i).getSecond());
            }

            System.out.println();

            //Close the posting lists
            for (PostingList postingList : postingLists) {
                postingList.closeList();
            }


        }

    }

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

        evaluateQueries(getQueries(), configuration, documentIndex, lexicon, true, true);

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
