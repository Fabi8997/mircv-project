package it.unipi.mircv.beans;

import java.io.*;

public class Configuration {
    final static String PATH = "Files/configuration.txt";

    private boolean stemmingAndStopwordsRemoval;

    private boolean compressed;

    public boolean loadConfiguration() {
        try {
            //creates a new file instance
            File file = new File(PATH);

            //reads the file
            FileReader fr = new FileReader(file);

            //creates a buffering character input stream
            BufferedReader br = new BufferedReader(fr);

            String line;

            if ((line = br.readLine()) != null) {
                stemmingAndStopwordsRemoval = Boolean.parseBoolean(line);
            }
            if ((line = br.readLine()) != null) {
                compressed = Boolean.parseBoolean(line);
            }

            fr.close();

            System.out.println(this);
        } catch (IOException e) {
            System.err.println("No indexing configuration found. Try to first create a new index, then start again the"+
                    " query processor.");
            return false;
        }
        return true;
    }

    /**
     * Write the configuration of the inverted index, in particular if the stemming and stopwords removal were enabled
     * and the same for compression.
     * @param stemmingAndStopwordsRemoval true if the stemming and stopwords removal were enabled during the indexing.
     * @param compressed true if the compression was enabled during the indexing.
     */
    public static void saveConfiguration(boolean stemmingAndStopwordsRemoval, boolean compressed){

        //Object used to build the lexicon line into a string
        StringBuilder stringBuilder = new StringBuilder();

        //Buffered writer used to format the output
        BufferedWriter bufferedWriter;

        try {
            bufferedWriter = new BufferedWriter(new FileWriter(PATH,false));

            //build the string
            stringBuilder
                    .append(stemmingAndStopwordsRemoval).append("\n")
                    .append(compressed).append("\n");

            //Write the string in the file
            bufferedWriter.write(stringBuilder.toString());

            //Close the writer
            bufferedWriter.close();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean getStemmingAndStopwordsRemoval() {
        return stemmingAndStopwordsRemoval;
    }

    public boolean getCompressed() {
        return compressed;
    }

    @Override
    public String toString() {
        return "Stemming and stopwords removal = " + stemmingAndStopwordsRemoval +
                "\nCompressed = " + compressed;
    }
}
