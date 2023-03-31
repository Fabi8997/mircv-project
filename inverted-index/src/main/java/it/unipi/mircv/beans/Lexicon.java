package it.unipi.mircv.beans;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.util.HashMap;

/**
 * Class to handle the loading of the lexicon from its file and keep it in memory
 */
public class Lexicon extends HashMap<String,TermInfo> {

    //Path of the lexicon file
    private final static String LEXICON_PATH = "src/main/resources/files/lexicon.txt";

    //Object to open the stream from the lexicon file
    private RandomAccessFile lexiconFile;

    /**
     * Constructor, it uses the HashMap constructor.
     */
    public Lexicon() {
        super();
    }

    /**
     * Load the lexicon in memory.
     */
    public void loadLexicon() {

        try {
            //Start the stream from the lexicon file
            lexiconFile = new RandomAccessFile(LEXICON_PATH, "r");

            //Accumulator for the current offset in the file
            int offset = 0;

            //Accumulator for the current termInfo reading
            TermInfo termInfo;

            //While we're not at the end of the file
            while (offset < lexiconFile.length()) {

                //Read the next termInfo from the file starting at the current offset
                termInfo = readNextTerm(offset);

                //If the termInfo is not null (no problem encountered, or we aren't at the end of the file)
                if (termInfo!= null){

                    //Insert the termInfo into the HashMap
                    this.put(termInfo.getTerm(), termInfo);

                    //Increment the offset
                    offset += TermInfo.TERM_INFO_LENGTH;
                }
            }

            System.out.println("[LEXICON LOADER] Lexicon loaded");

        } catch (Exception e) {
            System.out.println("[LEXICON LOADER] Error loading lexicon: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Read the next term from the lexicon file.
     * @param offset starting offset of the next term to be read
     * @return The next term from the lexicon file.
     */
    private TermInfo readNextTerm(int offset) {
        //Array of bytes in which put the term
        byte[] termBytes = new byte[TermInfo.TERM_LENGTH];

        //String containing the term
        String term;

        //TermInfo containing the term information to be returned
        TermInfo termInfo;

        try {
            //Set the file pointer to the start of the lexicon entry
            lexiconFile.seek(offset);

            //Read the first 48 containing the term
            lexiconFile.readFully(termBytes, 0, TermInfo.TERM_LENGTH);

            //Convert the bytes to a string and trim it
            term = new String(termBytes, Charset.defaultCharset()).trim();

            //Instantiate the TermInfo object reading the next 3 integers from the file
            termInfo = new TermInfo(term,   //Term
                    lexiconFile.readLong(),  //Offset docids file
                    lexiconFile.readLong(),  //Offset frequencies file
                    lexiconFile.readInt(),  //Length in bytes of the docids list
                    lexiconFile.readInt(),  //Length in bytes of the frequencies list
                    lexiconFile.readInt()); //Length of the term's posting list

            return termInfo;

        } catch (IOException e) {
            //System.err.println("[ReadNextTermInfo] EOF reached while reading the next lexicon entry");
            return null;
        }
    }

}
