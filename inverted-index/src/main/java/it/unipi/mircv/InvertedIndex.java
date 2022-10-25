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

public class InvertedIndex {

    static String COLLECTION_PATH = "src/main/resources/dataset/samplecompressed.tar.gz";


    // TODO: 25/10/2022 ParseColletion must read N lines, then it must write the partial indexes obtained parsing these
    // TODO: 25/10/2022 lines into a partial file in the disc -> e.g. read 10 lines, build the inverted index for each  
    // TODO: 25/10/2022 line using a tmp accumulator (probably an hashmap) then write the partial sorted inverted index 
    // TODO: 25/10/2022 in a file, then when all the files are created it must be performed the merge of these files
    /**
     * Build an inverted index for the collection in the given path
     * @param path Path of the archive containing the collection, must be a tar.gz archive
     * @param blockSize Size of the block to be read, that is the number of lines for each chunk of input document
     * @param stopwordsRemovalAndStemming true to apply the stopwords removal and stemming procedure, false otherwise
     * @return The inverted index
     */
    private static String parseCollection(String path, int blockSize, Boolean stopwordsRemovalAndStemming){

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

                //Counter to keep the number of blocks read
                int blockNumber = 1;

                //Counter to keep the number of lines currently read
                int linesRead = 0;

                //Iterate over the lines
                while ((line = bufferedReader.readLine()) != null ) {

                    //String to keep the current document processed
                    String processedDocument;

                    //Check it the stopwords removal and stemming are requested
                    if (stopwordsRemovalAndStemming) {

                        //Process the document using the stemming and stopwords removal
                        processedDocument = Parser.processDocument(line, true, true, stopwords);
                    } else {

                        //Process the document without the stemming and stopwords removal
                        processedDocument = Parser.processDocument(line, false, false, null);
                    }

                    //If the parsing of the document was completed correctly, it'll be appended to the collection buffer
                    if (processedDocument!= null && !processedDocument.isEmpty()) {

                        // TODO: 25/10/2022 Implement here the SPIMI algorithm
                        //Put each document in a different line: <doc id>\t<tokens>
                        stringBuffer.append(processedDocument);
                        stringBuffer.append('\n');


                        linesRead++;

                        //If we have all the lines to build a block, it will be built
                        if(linesRead == blockSize){
                            // TODO: 25/10/2022 Here we've to write the stringBuffer into a file
                            System.out.println("Block "+blockNumber+" written");
                            linesRead = 0;
                            blockNumber++;
                            System.out.println(stringBuffer);
                            stringBuffer.delete(0,stringBuffer.length());
                        }
                    }
                }
                //Last block that have a size in the range [1, N]
                if(linesRead > 0){
                    // TODO: 25/10/2022 Here we've to write the stringBuffer into a file
                    System.out.println("Block "+blockNumber+" written");
                    System.out.println(stringBuffer);
                    stringBuffer.delete(0,stringBuffer.length());
                }
            }

            //Close the input stream
            fileInputStream.close();

            // TODO: 25/10/2022 instead of returning, the partial inverted index must be written in a file 
            //Return the parsed collection
            return stringBuffer.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args){
        parseCollection(COLLECTION_PATH, 15, false);
    }
}
