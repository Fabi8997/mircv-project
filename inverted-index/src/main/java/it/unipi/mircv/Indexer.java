package it.unipi.mircv;

import it.unipi.mircv.beans.ParsedDocument;
import it.unipi.mircv.builder.InvertedIndexBuilder;
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

    //Path of the dataset
    static String COLLECTION_PATH = "src/main/resources/dataset/samplecompressed.tar.gz";

    //Statistic file path
    static final String STATISTICS_PATH = "src/main/resources/files/statistics.txt";

    //Document index file path
    static final String DOCUMENT_INDEX_PATH = "src/main/resources/files/document_index.txt";

    //Percentage of memory used to define a threshold
    static final double PERCENTAGE = 0.6;

    /**
     * Build an inverted index for the collection in the given path; it uses the SPIMI algorithm and build different
     * blocks containing each one a partial inverted index and the respective lexicon.
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
        try (FileInputStream fileInputStream = new FileInputStream(file);
             RandomAccessFile documentIndexFile = new RandomAccessFile(DOCUMENT_INDEX_PATH, "rw")) {

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

                //Retrieve the time at the beginning of the computation
                long begin = System.nanoTime();

                //Retrieve the initial free memory
                long initialMemory = Runtime.getRuntime().freeMemory();

                //Retrieve the total memory allocated for the execution of the current runtime
                long totalMemory = Runtime.getRuntime().totalMemory();

                //Retrieve the memory used at the beginning of the computation
                long beforeUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();

                //Define the threshold of memory over which the index must be flushed to disk
                long THRESHOLD = (long) (totalMemory * PERCENTAGE);

                System.out.println("[INDEXER] Initial total memory allocated "+ totalMemory/(1024*1024)+"MB");
                System.out.println("[INDEXER] Initial free memory "+ initialMemory/(1024*1024)+"MB");
                System.out.println("[INDEXER] Initial memory used "+ beforeUsedMem/(1024*1024)+"MB");
                System.out.println("[INDEXER] Memory threshold: " + THRESHOLD/(1024*1024)+"MB -> 40%");
                System.out.println("[INDEXER] Starting to fetch the documents...");

                //Iterate over the lines
                while ((line = bufferedReader.readLine()) != null ) {

                    // TODO: 26/10/2022 DEAL WITH THE WHITESPACE THAT MUST NOT BE PRESENT IN THE LEXICON!

                    //Process the document using the stemming and stopwords removal
                    parsedDocument = Parser.processDocument(line, stopwordsRemovalAndStemming, stopwords);

                    //If the parsing of the document was completed correctly, it'll be appended to the collection buffer
                    if (parsedDocument!= null && parsedDocument.getTerms().length != 0) {

                        //Increase the number of documents analyzed in total
                        numberOfDocuments++;

                        //Increase the number of documents analyzed in the current block
                        blockDocuments++;

                        //Set the current number of documents processed as the document identifier
                        parsedDocument.setDocId(numberOfDocuments);

                        //System.out.println("[INDEXER] Doc: "+parsedDocument.docId + " read with " + parsedDocument.documentLength + "terms");
                        invertedIndexBuilder.insertDocument(parsedDocument);

                        //Insert the document index row in the document index file. It's the building of the document
                        // index. The document index will be read from file in the future, the important is to build it
                        // and store it inside a file.
                        parsedDocument.writeToDisk(documentIndexFile);

                        //Check if the memory used is above the threshold defined
                        if(!isMemoryAvailable(THRESHOLD)){
                            System.out.println("[INDEXER] Memory over the threshold");
                            System.out.println("[INDEXER] Flushing " + blockDocuments + " documents to disk...");

                            //Sorting the lexicon and the inverted index
                            invertedIndexBuilder.sortLexicon();
                            invertedIndexBuilder.sortInvertedIndex();

                            //Write the inverted index and the lexicon in the file
                            writeToFiles(invertedIndexBuilder, blockNumber);

                            System.out.println("[INDEXER] Block "+blockNumber+" written to disk!");

                            //Handle the blocks' information
                            blockNumber++;
                            blockDocuments = 0;

                            //Clear the inverted index data structure and call the garbage collector
                            invertedIndexBuilder.clear();
                        }

                        //Print checkpoint information
                        if(numberOfDocuments%50000 == 0){
                            System.out.println("[INDEXER] " + numberOfDocuments+ " processed");
                            System.out.println("[INDEXER] Processing time: " + (System.nanoTime() - begin)/1000000000+ "s");
                        }
                    }
                }
                if(blockDocuments > 0 ){

                    System.out.println("[INDEXER] Last block reached");
                    System.out.println("[INDEXER] Flushing " + blockDocuments + " documents to disk...");

                    //Sort the lexicon and the inverted index
                    invertedIndexBuilder.sortLexicon();
                    invertedIndexBuilder.sortInvertedIndex();

                    //Write the inverted index and the lexicon in the file
                    writeToFiles(invertedIndexBuilder, blockNumber);

                    System.out.println("[INDEXER] Block "+blockNumber+" written to disk");

                    //Write the blocks statistics
                    writeStatistics(blockNumber, numberOfDocuments);

                    System.out.println("[INDEXER] Statistics of the blocks written to disk");

                }else{
                    //Write the blocks statistics
                    writeStatistics(blockNumber-1, numberOfDocuments);

                    System.out.println("[INDEXER] Statistics of the blocks written to disk");
                }

                //Close the random access file of the document index
                documentIndexFile.close();
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Write the statistics of the execution, in particular the number of blocks written and the total number of
     * documents parsed.
     * @param numberOfBlocks Number of blocks written
     * @param numberOfDocs Number of documents parsed in total
     */
    private static void writeStatistics(int numberOfBlocks, int numberOfDocs){

        //Object used to build the lexicon line into a string
        StringBuilder stringBuilder = new StringBuilder();

        //Buffered writer used to format the output
        BufferedWriter bufferedWriter;

        try {
            bufferedWriter = new BufferedWriter(new FileWriter(Indexer.STATISTICS_PATH,true));

            //build the string
            stringBuilder
                    .append(numberOfBlocks).append("\n")
                    .append(numberOfDocs).append("\n");

            //Write the string in the file
            bufferedWriter.write(stringBuilder.toString());

            //Close the writer
            bufferedWriter.close();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Write the inverted index and the lexicon blocks, the number of the block is passed as parameter. At the end
     * it clears the data structures and call the garbage collector
     * @param invertedIndexBuilder Inverted index builder object containing the inverted index and the lexicon
     * @param blockNumber Number of the block that will be written
     */
    private static void writeToFiles(InvertedIndexBuilder invertedIndexBuilder, int blockNumber){

        //Write the inverted index's files into the block's files
        invertedIndexBuilder.writeInvertedIndexToFile(
                "src/main/resources/files/invertedIndexDocIds"+blockNumber+".txt",
                "src/main/resources/files/invertedIndexFrequencies"+blockNumber+".txt");

        //Write the block's lexicon into the given file
        invertedIndexBuilder.writeLexiconToFile("src/main/resources/files/lexiconBlock"+blockNumber+".txt");

        System.out.println("Block "+blockNumber+" written");

        //Clear the inverted index and lexicon data structure and call the garbage collector
        invertedIndexBuilder.clear();
    }

    /**
     * Return true if the memory used is under the threshold, so there is enough free memory to continue the computation
     * otherwise it will return false.
     * @param threshold Memory threshold in byte.
     */
    private static boolean isMemoryAvailable(long threshold){

        //Subtract the free memory at the moment to the total memory allocated obtaining the memory used, then check
        //if the memory used is above the threshold
        return Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory() < threshold;
    }

    /*//For debug
    private static long getMemoryUsed(){
        Runtime rt = Runtime.getRuntime();
        long total_mem = rt.totalMemory();
        long free_mem = rt.freeMemory();
        return total_mem - free_mem;
    }*/


    public static void main(String[] args){
        //Create the inverted index
        parseCollection(COLLECTION_PATH, Boolean.valueOf(args[1]));

        // TODO: 25/03/2023 Merge the inverted index and the lexicon
    }
}
