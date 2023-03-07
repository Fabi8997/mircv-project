package it.unipi.mircv;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class InvertedIndex {

    static String COLLECTION_PATH = "src/main/resources/dataset/samplecompressed.tar.gz";


    // TODO: 25/10/2022 ParseCollection must read N lines, then it must write the partial indexes obtained parsing these
    // TODO: 25/10/2022 lines into a partial file in the disc -> e.g. read 10 lines, build the inverted index for each  
    // TODO: 25/10/2022 line using a tmp accumulator (probably an hashmap) then write the partial sorted inverted index 
    // TODO: 25/10/2022 in a file, then when all the files are created it must be performed the merge of these files
    /**
     * Build an inverted index for the collection in the given path
     * @param path Path of the archive containing the collection, must be a tar.gz archive
     * @param blockSize Size of the block to be read, that is the number of lines for each chunk of input document
     * @param stopwordsRemovalAndStemming true to apply the stopwords removal and stemming procedure, false otherwise
     */
    private static void parseCollection(String path, int blockSize, Boolean stopwordsRemovalAndStemming){

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

                //Counter to keep the number of blocks read
                int blockNumber = 1;

                //Counter to keep the number of documents read
                int numberOfDocuments = 0;

                //Counter to keep the number of lines currently read
                int linesRead = 0;

                //IndexBuilder used to build the inverted index for each block
                InvertedIndexBuilder invertedIndexBuilder = new InvertedIndexBuilder();


                //Iterate over the lines
                while ((line = bufferedReader.readLine()) != null ) {

                    // TODO: 26/10/2022 DEAL WITH THE WHITESPACE THAT MUST NOT BE PRESENT IN THE LEXICON!

                    //String to keep the current document processed
                    String processedDocument = null;


                    //Check it the stopwords removal and stemming are requested
                    if (stopwordsRemovalAndStemming) {

                        //Process the document using the stemming and stopwords removal
                        //processedDocument = Parser.processDocument(line, true, true, stopwords);
                    } else {

                        //Process the document without the stemming and stopwords removal
                        //processedDocument = Parser.processDocument(line, false, false, null);
                    }

                    //If the parsing of the document was completed correctly, it'll be appended to the collection buffer
                    if (processedDocument!= null && !processedDocument.isEmpty()) {

                        //Insert the document in the block's data structures (Lexicon and inverted index)
                        //inverterIndexBuilder.insertDocument(processedDocument);

                        //linesRead++;

                        //If we have all the lines to build a block, it will be built
                        if(!isMemoryAvailable(0.7)){

                            //Write all the block's information to files
                            writeToFiles(invertedIndexBuilder, blockNumber);

                            //Reset the number of lines read
                            //linesRead = 0;

                            numberOfDocuments = blockNumber*blockSize;

                            //Increment the id of the next block
                            blockNumber++;
                        }
                    }
                }
                //Last block that have a size in the range [1, N]
                // TODO: 11/11/2022 Check if the disk based buffer is not empty, if it isn't empty write the things
                if(linesRead > 0){

                    numberOfDocuments = (blockNumber -1)*blockSize + linesRead;
                    writeStatistics("src/main/resources/files/statistics", blockNumber, numberOfDocuments);

                    //Write all the block's information to files
                    writeToFiles(invertedIndexBuilder, blockNumber);
                }else{
                    writeStatistics("src/main/resources/files/statistics", blockNumber-1, numberOfDocuments);
                }

            }



        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private static void writeStatistics(String outputPath, int numberOfBlocks, int numberOfDocs){

        //Object used to build the lexicon line into a string
        StringBuilder stringBuilder = new StringBuilder();

        BufferedWriter bufferedWriter;
        try {
            bufferedWriter = new BufferedWriter(new FileWriter(outputPath,true));

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

        //Write the block's document index into the given file
        invertedIndexBuilder.writeDocumentIndexToFile("src/main/resources/files/documentIndex.txt");

        //Write the inverted index's files into the block's files
        invertedIndexBuilder.writeInvertedIndexToFile(
                "src/main/resources/files/invertedIndexDocIds"+blockNumber+".txt",
                "src/main/resources/files/invertedIndexFrequencies"+blockNumber+".txt");

        System.out.println("Block "+blockNumber+" written");

        //Clear the data structures
        invertedIndexBuilder.clear();
    }

    private static boolean isMemoryAvailable(double percentage){
        Runtime rt = Runtime.getRuntime();
        long total_mem = rt.totalMemory();
        long free_mem = rt.freeMemory();
        long used_mem = total_mem - free_mem;
        long percentage_used_mem = used_mem/total_mem;
        System.out.println("Amount of used memory: " + percentage_used_mem + "%");
        return (percentage_used_mem < percentage);
    }

    public static void main(String[] args){
        parseCollection(COLLECTION_PATH, Integer.parseInt(args[0]), Boolean.valueOf(args[1]));

    }
}
