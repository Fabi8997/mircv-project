package it.unipi.mircv.parser;

import opennlp.tools.stemmer.PorterStemmer;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Parser {

    static String COLLECTION_PATH = "src/main/resources/dataset/samplecompressed.tar.gz";

    /**
     * Parse the collection tokenizing each document and saving it in the  format: doc id text_tokenized
     * @param path Path of the archive containing the collection, must be a tar.gz archive
     * @param stopwordsRemovalAndStemming true to apply the stopwords removal and stemming procedure, false otherwise
     * @return The collection parsed into a string
     */
    private static String parseCollection(String path, Boolean stopwordsRemovalAndStemming){

        //Path of the collection to be read
        File file = new File(path);

        //List of strings that will contain the stopwords for the stopwords removal procedure
        List<String> stopwords = null;

        //Object used to manipulate and build strings
        StringBuilder stringBuffer = new StringBuilder();

        //If the stopwords removal and the stemming is requested, the stopwords are read from a file
        if(stopwordsRemovalAndStemming) {
            try {
                stopwords = Files.readAllLines(Paths.get("src/main/resources/utility/stopwords-en.txt"));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        //Try to open the collection provided
        try (FileInputStream fileInputStream = new FileInputStream(file)) {

            //Create an input stream for the tar archive
            TarArchiveInputStream tarInput = new TarArchiveInputStream(new GzipCompressorInputStream(fileInputStream));

            //Get the first file from the stream, that is only one
            TarArchiveEntry currentEntry = tarInput.getNextTarEntry();

            //If the file exist
            if(currentEntry != null) {

                //Read the uncompressed tar file specifying UTF-8 as encoding
                InputStreamReader inputStreamReader = new InputStreamReader(tarInput, StandardCharsets.UTF_8);

                //Create a BufferedReader in order to access one line of the file at a time
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                //Variable to keep the current line read from the buffer
                String line;

                //Iterate over the lines
                while ((line = bufferedReader.readLine()) != null) {

                    //String to keep the current document processed
                    String processedDocument;

                    //Check it the stopwords removal and stemming are requested
                    if (stopwordsRemovalAndStemming) {

                        //Process the document using the stemming and stopwords removal
                        processedDocument = processDocument(line, true, true, stopwords);
                    } else {

                        //Process the document without the stemming and stopwords removal
                        processedDocument = processDocument(line, false, false, null);
                    }

                    //If the parsing of the document was completed correctly, it'll be appended to the collection buffer
                    if (processedDocument!= null && !processedDocument.isEmpty()) {

                        //Put each document in a different line: <doc id>\t<tokens>
                        stringBuffer.append(processedDocument);
                        stringBuffer.append('\n');
                    }
                }
            }

            //Close the input stream
            fileInputStream.close();

            //Return the parsed collection
            return stringBuffer.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Parse the collection tokenizing each document in the format: doc_id text_tokenized
     * @param line String containing a document of the collection in the format: [doc_id]\t[text]\n
     * @param stopwordsRemoval True to perform the stopwords removal, otherwise must be False
     * @param stemming True to perform the stemming, otherwise must be False
     * @param stopwords List of strings containing the stopwords, it's Null if stopwordsRemoval is false
     * @return Document tokenized in the format: [doc_id]\t[token1 token2 ... tokenN]\n
     */
    private static String processDocument(String line, boolean stopwordsRemoval, boolean stemming, List<String> stopwords){
        //Utility variables to keep the current docno and text
        String docno;
        String text;

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

        //Create an array list of stems by computing different phases from a stream of tokens:ù
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
        System.out.println(Parser.parseCollection(COLLECTION_PATH, Boolean.valueOf(args[1])));
    }
}
