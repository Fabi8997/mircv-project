package it.unipi.mircv.parser;

import it.unipi.mircv.beans.ParsedDocument;
import opennlp.tools.stemmer.PorterStemmer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Parser {

    //List of strings containing the stopwords
    static List<String> stopWords = loadStopWords();

    //Path to the file containing the list of stopwords
    static final String STOPWORDS_FILE = "Resources/utility/stopwords-en.txt";

    /**
     * Parse the document tokenizing each document in the format: doc_id text_tokenized
     * @param line String containing a document of the collection in the format: [doc_id]\t[text]\n
     * @param stopwordsRemovalAndStemming True to perform the stopwords removal and stemming, otherwise must be False
     * @return Document tokenized in the format: [doc_id]\t[token1 token2 ... tokenN]\n
     */
    public static ParsedDocument processDocument(String line, boolean stopwordsRemovalAndStemming){
        //Utility variables to keep the current docno and text
        String docno;
        String text;

        // TODO: 03/11/2022 check if in the complete collection there are badly formatted files

        //Divide the line using \t as delimiter, it'll split the doc_id and the text
        StringTokenizer stringTokenizer = new StringTokenizer(line, "\t");

        //Retrieve the first token, that is the docno
        if(stringTokenizer.hasMoreTokens()){
            docno = stringTokenizer.nextToken();

            //Retrieve the second token, that is the text and cast it to lower case
            if(stringTokenizer.hasMoreTokens()){
                text = stringTokenizer.nextToken().toLowerCase();
            }else{
                //The text is empty, or it was not possible to retrieve it
                return null;
            }
        }else{
            //The line is empty, or it was not possible to retrieve it
            return null;
        }


        //Remove punctuation, then split when one or more whitespace characters occur
        String[] splittedText = removePunctuation(text).split("\\s+");

        if(stopwordsRemovalAndStemming) {
            //Remove stop words
            splittedText = removeStopWords(splittedText, stopWords);

            //Stemming
            splittedText = getStems(splittedText);
        }

        //System.out.println("[DEBUG: PARSING DOCUMENT] " + (System.currentTimeMillis() - begin) + "ms");

        return new ParsedDocument(docno, splittedText);
        
    }

    /**
     * Remove the punctuation by replacing it with an empty string
     * @param text String containing a text
     * @return Text without punctuation
     */
    private static String removePunctuation(String text){
        //Replace all punctuation marks with a whitespace character, then trim the string to remove the whitespaces
        // at the beginning or end of the string.
        return text.replaceAll("[^\\w\\s]", " ").trim();
    }

    /**
     * Remove the given stopwords from the text
     * @param text String containing a text
     * @param stopwords List of strings containing the stopwords
     * @return Text without the stopwords
     */
    private static String[] removeStopWords(String[] text, List<String> stopwords){

        //Using the streams is better, since the performance are x6 faster than the manual remove or regex removal
        ArrayList<String> words = Stream.of(text)
                .collect(Collectors.toCollection(ArrayList<String>::new));
        words.removeAll(stopwords);
        String[] terms = new String[words.size()];
        return words.toArray(terms);
    }

    /**
     * Apply the Porter Stemmer in order to stem each token in a text
     * @param terms Array of String containing a tokenized text
     * @return Array of stems
     */
    private static String[] getStems(String[] terms){

        //Instance of a porter stemmer
        PorterStemmer porterStemmer = new PorterStemmer();

        //Create an array list of stems by computing different phases from a stream of tokens:
        //  The stream is obtained by splitting the text using the whitespace as delimiter;
        //  It's used a map stage where each word is stemmed
        //  The overall result is collected into an Array of strings
        return Stream.of(terms)
                .map(porterStemmer::stem).toArray(String[]::new);
    }

    private static List<String> loadStopWords(){
        System.out.println("[PARSER] Loading stop words...");
        //If the stopwords removal and the stemming is requested, the stopwords are read from a file
        try {
            return Files.readAllLines(Paths.get(STOPWORDS_FILE));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
