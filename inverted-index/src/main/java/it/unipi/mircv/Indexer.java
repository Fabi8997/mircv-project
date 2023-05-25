package it.unipi.mircv;

import it.unipi.mircv.beans.Configuration;
import it.unipi.mircv.beans.DocumentIndexEntry;
import it.unipi.mircv.beans.ParsedDocument;
import it.unipi.mircv.beans.Statistics;
import it.unipi.mircv.builder.InvertedIndexBuilder;
import it.unipi.mircv.merger.IndexMerger;
import it.unipi.mircv.parser.Parser;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class Indexer {

    //Counter to keep the number of blocks read
    static int blockNumber = 1;

    //Path of the dataset
    static String COLLECTION_PATH = "Dataset/samplecompressed.tar.gz";

    static final String FILES_PATH = "Files/";
    //Document index file path
    static final String DOCUMENT_INDEX_PATH = "Files/document_index.txt";

    static final String LEXICON_BLOCK_PATH = "inverted-index/src/main/resources/tmp/lexiconBlock";

    static final String DOCIDS_BLOCK_PATH = "inverted-index/src/main/resources/tmp/invertedIndexDocIds";

    static final String FREQUENCIES_BLOCK_PATH = "inverted-index/src/main/resources/tmp/invertedIndexFrequencies";

    static final String ARGS_ERROR = "Select one from the following arguments:\n" +
            "-s : stemming and stopwords removal enabled\n" +
            "-c : compression enabled\n" +
            "-sc : both enabled";

    //Percentage of memory used to define a threshold
    static final double PERCENTAGE = 0.5;

    /**
     * Build an inverted index for the collection in the given path; it uses the SPIMI algorithm and build different
     * blocks containing each one a partial inverted index and the respective lexicon.
     * @param path Path of the archive containing the collection, must be a tar.gz archive
     * @param stopwordsRemovalAndStemming true to apply the stopwords removal and stemming procedure, false otherwise
     */
    private static void parseCollection(String path, Boolean stopwordsRemovalAndStemming, Boolean debug){

        //Path of the collection to be read
        File file = new File(path);

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

                //variable to keep track of the average length of the document
                float avdl = 0;

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
                System.out.println("[INDEXER] Memory threshold: " + THRESHOLD/(1024*1024)+"MB -> " + PERCENTAGE * 100 + "%");
                System.out.println("[INDEXER] Starting to fetch the documents...");

                //Iterate over the lines
                while ((line = bufferedReader.readLine()) != null ) {

                    //Process the document using the stemming and stopwords removal
                    parsedDocument = Parser.processDocument(line, stopwordsRemovalAndStemming);

                    //If the parsing of the document was completed correctly, it'll be appended to the collection buffer
                    if (parsedDocument!= null && parsedDocument.getTerms().length != 0) {

                        //updating the average number of documents
                        avdl = avdl*(numberOfDocuments)/(numberOfDocuments + 1) + ((float) parsedDocument.getTerms().length)/(numberOfDocuments + 1);

                        //Increase the number of documents analyzed in total
                        numberOfDocuments++;

                        //Increase the number of documents analyzed in the current block
                        blockDocuments++;

                        //Set the docid of the current document
                        parsedDocument.setDocId(numberOfDocuments);

                        //System.out.println("[INDEXER] Doc: "+parsedDocument.docId + " read with " + parsedDocument.documentLength + "terms");
                        invertedIndexBuilder.insertDocument(parsedDocument);

                        //Insert the document index row in the document index file. It's the building of the document
                        // index. The document index will be read from file in the future, the important is to build it
                        // and store it inside a file.
                        DocumentIndexEntry docEntry = new DocumentIndexEntry(parsedDocument.getDocNo(), parsedDocument.getDocumentLength());
                        docEntry.writeToDisk(documentIndexFile, numberOfDocuments);

                        //Check if the memory used is above the threshold defined
                        if(!isMemoryAvailable(THRESHOLD)){
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
                            if(debug) {
                                System.out.println("[DEBUG] Document index entry: " + docEntry);
                                System.out.println("[DEBUG] Memory used: " + getMemoryUsed()*100 + "%");
                            }
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
                    Statistics.writeStatistics(blockNumber, numberOfDocuments, avdl);

                    System.out.println("[INDEXER] Statistics of the blocks written to disk");

                }else{
                    //Write the blocks statistics
                    Statistics.writeStatistics(blockNumber-1, numberOfDocuments, avdl);

                    System.out.println("[INDEXER] Statistics of the blocks written to disk");
                }

                System.out.println("[INDEXER] Total processing time: " + (System.nanoTime() - begin)/1000000000+ "s");
            }

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
                DOCIDS_BLOCK_PATH+blockNumber+".txt",
                FREQUENCIES_BLOCK_PATH +blockNumber+".txt");

        //Write the block's lexicon into the given file
        invertedIndexBuilder.writeLexiconToFile(LEXICON_BLOCK_PATH+blockNumber+".txt");

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

    /**
     * Method to clear the Files folder
     */
    private static void clearFiles(){
        try {
            FileUtils.cleanDirectory(new File(FILES_PATH));
        } catch (IOException e) {
            System.out.println("Error deleting files inside Files folder");
            throw new RuntimeException(e);
        }
    }

    /**
     * method to get the amount of memory used with respect to the memory available to the process
     * @return the amount of memory used with respect to the memory available to the process
     */
    private static long getMemoryUsed(){
        Runtime rt = Runtime.getRuntime();
        long total_mem = rt.totalMemory();
        long free_mem = rt.freeMemory();
        return  (total_mem - free_mem)/total_mem;
    }


    public static void main(String[] args){

        boolean stemmingAndStopwordsRemoval = false;
        boolean compressed = false;
        boolean debug = false;

        if(args.length >= 1){
            switch (args[0]) {
                case "-s":
                    stemmingAndStopwordsRemoval = true;
                    break;
                case "-c":
                    compressed = true;
                    break;
                case "-sc":
                    stemmingAndStopwordsRemoval = true;
                    compressed = true;
                    break;
                case "-d":
                    debug = true;
                    break;
                default:
                    System.err.println("Invalid command\n"+ARGS_ERROR);
                    return;
            }
        }

        if(args.length == 2){
            if(args[1].equals("-d") && !args[0].equals("-d")){
                debug = true;
            }
            else{
                System.err.println("Invalid command\n"+ARGS_ERROR);
            }
        }
        else if(args.length > 2){
            System.err.println("Wrong number of arguments\n"+ARGS_ERROR);
            return;
        }

        System.out.println("[INDEXER] Configuration\n" +
                "\tStemming and stopwords removal: " + stemmingAndStopwordsRemoval+"\n" +
                "\tCompression: " + compressed + "\n" +
                "\tDebug: " + debug);

        clearFiles();

        //Create the inverted index. Creates document index file and statistics file
        parseCollection(COLLECTION_PATH, stemmingAndStopwordsRemoval, debug);

        //Merge the blocks to obtain the inverted index, compressed indicates if the compression is enabled
        IndexMerger.merge(compressed, debug);

        System.out.println("[INDEXER] Saving execution configuration...");
        Configuration.saveConfiguration(stemmingAndStopwordsRemoval, compressed, debug);

        System.out.println("[INDEXER] Configuration saved");

    }
}
