package it.unipi.mircv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class Statistics {

    final String PATH = "src/main/resources/files/statistics";
    int numberOfBlocks;
    int numberOfDocuments;

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
            fr.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}