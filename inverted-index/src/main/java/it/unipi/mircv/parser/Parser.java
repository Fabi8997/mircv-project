package it.unipi.mircv.parser;

import opennlp.tools.stemmer.PorterStemmer;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Parser {

    private static String parseCollection(String path, Boolean stopwordsRemovalAndStemming){
        File file = new File(path);

        List<String> stopwords = null;
        if(stopwordsRemovalAndStemming) {
            try {
                stopwords = Files.readAllLines(Paths.get("src/main/resources/utility/stopwords-en.txt"));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        StringBuilder stringBuffer = new StringBuilder();
        try (FileInputStream fileInputStream = new FileInputStream(file)) {
            InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, StandardCharsets.UTF_8);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String line;
            while((line = bufferedReader.readLine())!= null){
                String processedDocument;

                if(stopwordsRemovalAndStemming) {
                    processedDocument = processDocument(line, stopwords);
                }else{
                    processedDocument = processDocument(line);
                }

                if(!processedDocument.isEmpty()){
                    stringBuffer.append(processedDocument);
                    stringBuffer.append('\n');
                }
            }
            fileInputStream.close();
            return stringBuffer.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String processDocument(String line){
        String[] splits = line.split("\t");
        String docno = splits[0];
        String text = splits[1].toLowerCase();

        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(docno).append("\t");

        //Remove punctuation
        text = removePunctuation(text);

        stringBuilder.append(text);

        return stringBuilder.toString();
    }
    private static String processDocument(String line, List<String> stopwords){
        String[] splits = line.split("\t");
        String docno = splits[0];
        String text = splits[1].toLowerCase();

        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(docno).append("\t");

        //Remove punctuation
        text = removePunctuation(text);

        //Remove stop words
        text = removeStopWords(text, stopwords);

        //Stemming
        text = getStems(text);

        stringBuilder.append(text);

        return stringBuilder.toString();

        // TODO: 19/10/2022 return this if the document isn't valid
        /*if(text.isEmpty())
            return "";*/
    }

    private static String removePunctuation(String text){
        return text.replaceAll("[^\\w\\s]","");
    }

    private static String removeStopWords(String text, List<String> stopwords){
        ArrayList<String> words = Stream.of(text.split(" "))
                .collect(Collectors.toCollection(ArrayList<String>::new));
        words.removeAll(stopwords);
        return String.join(" ", words);
    }

    private static String getStems(String text){
        PorterStemmer porterStemmer = new PorterStemmer();
        ArrayList<String> words = Stream.of(text.split(" "))
                .map(porterStemmer::stem)
                .collect(Collectors.toCollection(ArrayList<String>::new));
        return String.join(" ", words);
    }

    public static void main(String[] args) {
        //TEST parseCollection
        //System.out.println(Parser.parseCollection("src/main/resources/dataset/sample.tsv", false));
        System.out.println(Parser.parseCollection("src/main/resources/dataset/sample.tsv", true));
    }
}
