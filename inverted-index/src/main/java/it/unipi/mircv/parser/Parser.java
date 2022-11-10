package it.unipi.mircv.parser;

import opennlp.tools.stemmer.PorterStemmer;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Parser {

    /**
     * Parse the document tokenizing each document in the format: doc_id text_tokenized
     * @param line String containing a document of the collection in the format: [doc_id]\t[text]\n
     * @param stopwordsRemoval True to perform the stopwords removal, otherwise must be False
     * @param stemming True to perform the stemming, otherwise must be False
     * @param stopwords List of strings containing the stopwords, it's Null if stopwordsRemoval is false
     * @return Document tokenized in the format: [doc_id]\t[token1 token2 ... tokenN]\n
     */
    public static String processDocument(String line, boolean stopwordsRemoval, boolean stemming, List<String> stopwords){
        //Utility variables to keep the current docno and text
        String docno;
        String text;

        // TODO: 03/11/2022 firstly check if the line is not malformed si is in the format <docno>\t<text>\n

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

        //Object to build the result string
        StringBuilder stringBuilder = new StringBuilder();

        //Build the first part: [docno]\t
        stringBuilder.append(docno).append("\t");

        // TODO: 26/10/2022 Fare tre metodi per gestire in un unico stream (Forse non si può)!!!

        //Remove punctuation
        text = removePunctuation(text);

        if(stopwordsRemoval) {
            //Remove stop words
            text = removeStopWords(text, stopwords);
        }

        if(stemming) {
            //Stemming
            text = getStems(text);
        }

        //Build the second part: [docno]\t[token1 token2 ... tokenN]\n
        stringBuilder.append(text);

        return stringBuilder.toString();
        
    }

    /**
     * Remove the punctuation by replacing it with an empty string
     * @param text String containing a text
     * @return Text without punctuation
     */
    private static String removePunctuation(String text){
        return text.replaceAll("[^\\w\\s]", "");
    }

    /**
     * Remove the given stopwords from the text
     * @param text String containing a text
     * @param stopwords List of strings containing the stopwords
     * @return Text without the stopwords
     */
    private static String removeStopWords(String text, List<String> stopwords){

        //Using the streams is better, since the performance are x6 faster than the manual remove or regex removal
        ArrayList<String> words = Stream.of(text.split(" "))
                .collect(Collectors.toCollection(ArrayList<String>::new));
        words.removeAll(stopwords);
        return String.join(" ", words);
    }

    /**
     * Apply the Porter Stemmer in order to stem each token in a text
     * @param text String containing a tokenized text
     * @return String composed by a list of stems
     */
    private static String getStems(String text){

        //Instance of a porter stemmer
        PorterStemmer porterStemmer = new PorterStemmer();

        //Create an array list of stems by computing different phases from a stream of tokens:
        //  The stream is obtained by splitting the text using the whitespace as delimiter;
        //  It's used a map stage where each word is stemmed
        //  The overall result is collected into an ArrayList of strings
        ArrayList<String> words = Stream.of(text.split(" "))
                .map(porterStemmer::stem)
                .collect(Collectors.toCollection(ArrayList<String>::new));

        //The words are joined together using a whitespace as delimiter
        return String.join(" ", words);
    }

    public static void main(String[] args) {
        //TEST parseCollection
        //System.out.println(Parser.parseCollection("src/main/resources/dataset/sample.tsv", false));
    }
}
