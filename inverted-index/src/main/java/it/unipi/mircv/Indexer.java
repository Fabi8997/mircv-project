package it.unipi.mircv;

import it.unipi.mircv.parser.Parser;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class Indexer {

    //Counter to keep the number of blocks read
    static int blockNumber = 1;

    static String COLLECTION_PATH = "src/main/resources/dataset/samplecompressed.tar.gz";

    static final String STATISTICS_PATH = "src/main/resources/files/statistics.txt";

    static final String DOCUMENT_INDEX_PATH = "src/main/resources/files/document_index.txt";

    static final double PERCENTAGE = 0.5;

    /**
     * Build an inverted index for the collection in the given path
     * @param path Path of the archive containing the collection, must be a tar.gz archive
     * @param stopwordsRemovalAndStemming true to apply the stopwords removal and stemming procedure, false otherwise
     */
    private static void parseCollection(String path, Boolean stopwordsRemovalAndStemming){

        //Path of the collection to be read
        File file = new File(path);

        //List of strings that will contain the stopwords for the stopwords removal procedure
        List<String> stopwords = null;

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

                //Instantiate the inverted index builder for the current block
                InvertedIndexBuilder invertedIndexBuilder = new InvertedIndexBuilder();

                //Counter to keep the number of documents read in total
                int numberOfDocuments = 0;

                //Counter to keep the number of documents read for the current block
                int blockDocuments = 0;

                //String to keep the current document processed
                ParsedDocument parsedDocument;

                System.out.println("[INDEXER] Starting to fetch the documents...");

                //Iterate over the lines
                while ((line = bufferedReader.readLine()) != null ) {

                    // TODO: 26/10/2022 DEAL WITH THE WHITESPACE THAT MUST NOT BE PRESENT IN THE LEXICON!

                    //Process the document using the stemming and stopwords removal
                    parsedDocument = Parser.processDocument(line, stopwordsRemovalAndStemming, stopwords);

                    //If the parsing of the document was completed correctly, it'll be appended to the collection buffer
                    if (parsedDocument!= null && parsedDocument.terms.length != 0) {

                        numberOfDocuments++;
                        blockDocuments++;

                        parsedDocument.setDocId(numberOfDocuments);

                        System.out.println("[INDEXER] Doc: "+parsedDocument.docId + " read with " + parsedDocument.documentLength + "terms");
                        invertedIndexBuilder.insertDocument(parsedDocument);

                        //Insert the document index row in the document index file. It's the building of the document
                        // index. The document index will be read from file in the future, the important is to build it
                        // and store it inside a file.

                        parsedDocument.writeToDisk(DOCUMENT_INDEX_PATH);

                        if(!isMemoryAvailable()){
                            System.out.println("[INDEXER] Memory over the threshold");
                            // TODO: 22/12/2022  
                            invertedIndexBuilder.sortLexicon();
                            invertedIndexBuilder.sortInvertedIndex();
                            invertedIndexBuilder.writeBlockToDisk(blockNumber);
                            System.out.println("[INDEXER] Block "+blockNumber+" written to disk");
                            blockNumber++;
                            blockDocuments = 0;
                        }
                    }
                }
                if(blockDocuments > 0 ){

                    // TODO: 22/12/2022
                    //invertedIndexBuilder.sortLexicon();
                    //invertedIndexBuilder.writeBlockToDisk(blockNumber);
                    System.out.println("[INDEXER] Block "+blockNumber+" written to disk");

                    writeStatistics(blockNumber, numberOfDocuments);
                    System.out.println("[INDEXER] Statistics of the blocks written to disk");

                }else{
                    writeStatistics(blockNumber-1, numberOfDocuments);
                    System.out.println("[INDEXER] Statistics of the blocks written to disk");
                }
                System.out.println("Document index: "+invertedIndexBuilder.documentIndex);
                System.out.println("Inverted index: "+invertedIndexBuilder.invertedIndex);
                System.out.println("Lexicon: " + invertedIndexBuilder.lexicon);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private static void writeStatistics(int numberOfBlocks, int numberOfDocs){

        //Object used to build the lexicon line into a string
        StringBuilder stringBuilder = new StringBuilder();

        BufferedWriter bufferedWriter;
        try {
            bufferedWriter = new BufferedWriter(new FileWriter(Indexer.STATISTICS_PATH,true));

            stringBuilder
                    .append(numberOfBlocks).append("\n")
                    .append(numberOfDocs).append("\n");

            bufferedWriter.write(stringBuilder.toString());

            bufferedWriter.close();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private static void writeToFiles(InvertedIndexBuilder invertedIndexBuilder, int blockNumber){
        //Write the block's lexicon into the given file
        invertedIndexBuilder.writeLexiconToFile("src/main/resources/files/lexiconBlock"+blockNumber+".txt");

        //Write the inverted index's files into the block's files
        invertedIndexBuilder.writeInvertedIndexToFile(
                "src/main/resources/files/invertedIndexDocIds"+blockNumber+".txt",
                "src/main/resources/files/invertedIndexFrequencies"+blockNumber+".txt");

        System.out.println("Block "+blockNumber+" written");

        //Clear the data structures
        invertedIndexBuilder.clear();
    }

    private static boolean isMemoryAvailable(){
        Runtime rt = Runtime.getRuntime();
        long total_mem = rt.totalMemory();
        long free_mem = rt.freeMemory();
        long used_mem = total_mem - free_mem;
        long percentage_used_mem = used_mem/total_mem;
        //System.out.println("Amount of used memory: " + percentage_used_mem + "%");
        return (percentage_used_mem < Indexer.PERCENTAGE);
    }


    public static void main(String[] args){
        parseCollection(COLLECTION_PATH, Boolean.valueOf(args[1]));


    }
}
