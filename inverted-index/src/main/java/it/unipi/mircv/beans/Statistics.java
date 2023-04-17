package it.unipi.mircv.beans;
import java.io.*;

public class Statistics {

    final static String PATH = "Files/statistics.txt";
    private int numberOfBlocks;
    private int numberOfDocuments;

    private int avdl;

    @Override
    public String toString() {
        return "Statistics{" +
                "numberOfBlocks=" + numberOfBlocks +
                ", numberOfDocuments=" + numberOfDocuments +
                ", avdl=" + avdl +
                '}';
    }

    public Statistics() {
        try {
            //creates a new file instance
            File file = new File(PATH);

            //reads the file
            FileReader fr = new FileReader(file);

            //creates a buffering character input stream
            BufferedReader br = new BufferedReader(fr);

            String line;

            if ((line = br.readLine()) != null) {
                numberOfBlocks = Integer.parseInt(line);
            }
            if ((line = br.readLine()) != null) {
                numberOfDocuments = Integer.parseInt(line);
            }
            if ((line = br.readLine())!= null) {
                avdl = Integer.parseInt(line);
            }
            fr.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Write the statistics of the execution, in particular the number of blocks written and the total number of
     * documents parsed.
     * @param numberOfBlocks Number of blocks written
     * @param numberOfDocs Number of documents parsed in total
     */
    public static void writeStatistics(int numberOfBlocks, int numberOfDocs, float avdl){

        //Object used to build the lexicon line into a string
        StringBuilder stringBuilder = new StringBuilder();

        //Buffered writer used to format the output
        BufferedWriter bufferedWriter;

        try {
            bufferedWriter = new BufferedWriter(new FileWriter(PATH,false));

            //build the string
            stringBuilder
                    .append(numberOfBlocks).append("\n")
                    .append(numberOfDocs).append("\n")
                    .append(Math.round(avdl)).append("\n");

            //Write the string in the file
            bufferedWriter.write(stringBuilder.toString());

            //Close the writer
            bufferedWriter.close();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int getNumberOfBlocks() {
        return numberOfBlocks;
    }

    public int getNumberOfDocuments() { return numberOfDocuments; }

    public int getAvdl() { return  avdl; }

}