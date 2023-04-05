package it.unipi.mircv.beans;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class Statistics {

    final String PATH = "Files/statistics.txt";
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

    public int getNumberOfBlocks() {
        return numberOfBlocks;
    }

    public int getNumberOfDocuments() { return numberOfDocuments; }

    public int getAvdl() { return  avdl; }

}