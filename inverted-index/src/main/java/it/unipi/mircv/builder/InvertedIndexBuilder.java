package it.unipi.mircv.builder;

import it.unipi.mircv.beans.ParsedDocument;
import it.unipi.mircv.beans.Posting;
import it.unipi.mircv.beans.TermInfo;
import it.unipi.mircv.utils.Utils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Represent a component that gives the methods to build the lexicon and the inverted index for each block.
 */
public class InvertedIndexBuilder {

    //The lexicon has a String as a key and an array of integers as value, the value is composed by:
    // value[0] -> TermId
    // value[1] -> offset in the posting list
    HashMap<String, TermInfo> lexicon;
    HashMap<String, ArrayList<Posting>> invertedIndex;

    /**
     * Constructor of the class.
     * Instantiate the HashMap for the lexicon and the inverted index, used for the fast lookup that requires O(1);
     * Set the first termID to 1, the term id for each block carries also the information about the position of a term
     * in the inverted index.
     */
    public InvertedIndexBuilder() {
        lexicon = new HashMap<>();
        invertedIndex = new HashMap<>();
    }

    /**
     * Insert the document's tokens inside the lexicon and the inverted index, it's an implementation of SPIMI
     * @param parsedDocument Contains the id of the document, its length and the list of tokens
     */
    public void insertDocument(ParsedDocument parsedDocument) {

        //long begin = System.currentTimeMillis();
        //Generate a stream of String
        Stream.of(parsedDocument.getTerms())
                .forEach((term) -> {
                    //If the term is already present in the lexicon
                    if(lexicon.containsKey(term)){

                        //Retrieve the posting list of the term accessing the first element of the array of int that is
                        // the value of the termID in the lexicon
                        ArrayList<Posting> termPostingList = invertedIndex.get(term);

                        //Flag to set if the doc id's posting is present in the posting list of the term
                        boolean found = false;

                        //Iterate through the posting
                        for(Posting p : termPostingList){

                            //If the doc id is present, increment the frequency and terminate the loop
                            if(p.getDoc_id() == parsedDocument.getDocId()){

                                //Increment the frequency of the doc id
                                p.incrementFrequency();

                                found = true;
                                break;
                            }
                        }

                        //If the posting of the document is not present in the posting list, it must be added
                        if(!found){

                            //Posting added to the posting list of the term
                            termPostingList.add(new Posting(parsedDocument.getDocId(), 1));
                        }
                    }
                    //If the term was not present in the lexicon
                    else{
                        //Insert a new element in the lexicon, in each block the currTermID corresponds to the id
                        // associated to the term, but also to the position in the inverted index!
                        // To access the posting list of that term we can just retrieve the currTermId and access the
                        // array of posting lists
                        lexicon.put(term, new TermInfo() );

                        //Insert a new posting list in the inverted index
                        ArrayList<Posting> postingsList = new ArrayList<>();
                        Posting posting = new Posting(parsedDocument.getDocId(), 1);
                        postingsList.add(posting);

                        //Insert the new posting list
                        invertedIndex.put(term, postingsList);

                    }
                });
        //System.out.println("[DEBUG: INSERT DOCUMENT] " + (System.currentTimeMillis() - begin) + "ms");
    }

    /**
     * Clear the instance of the lexicon, it must be used after the lexicon has been written in the disk.
     */
    private void clearLexicon(){
        lexicon.clear();
    }

    /**
     * Clear the instance of the inverted index, it must be used after the inverted index has been written in the disk.
     */
    private void clearInvertedIndex(){
        invertedIndex.clear();
    }

    /**
     * Clear the class instances in order to be used for a new block processing.
     */
    public void clear(){
        clearLexicon();
        clearInvertedIndex();

        //Call the garbage collector to thrash the data structures cleared above, if it is not done the memory will be
        // over the threshold until the gc will be called automatically, causing the writes of a block at every document
        // processed after the trespassing of the threshold.
        Runtime.getRuntime().gc();
    }

    /**
     * Sort the lexicon with complexity O(nlog(n)) where n is the # of elements in the lexicon.
     */
    public void sortLexicon(){

        //To not double the memory instantiating a new data structure we've decided to use the following sorting
        lexicon = lexicon.entrySet()
                         .stream()
                         .sorted(Map.Entry.comparingByKey())
                         .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue,
                                (e1, e2) -> e1, LinkedHashMap::new)); //LinkedHashMap to keep O(1) time complexity

    }
    /**
     * Sort the inverted index with complexity O(nlog(n)) where n is the # of elements in the inverted index.
     */
    public void sortInvertedIndex(){

        invertedIndex = invertedIndex.entrySet()
                                     .stream()
                                     .sorted(Map.Entry.comparingByKey())
                                     .collect(Collectors.toMap(
                                            Map.Entry::getKey,
                                            Map.Entry::getValue,
                                            (e1, e2) -> e1, LinkedHashMap::new));

    }

    /**
     * Writes the current lexicon into a file
     * @param outputPath path of the file that will contain the block's lexicon
     */
    public void writeLexiconToFile(String outputPath){

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(outputPath, "rw")){

            //System.out.println("SORTED: \n" + sortedLexicon);

            lexicon.forEach( (key, termInfo) -> {

                //Fill with whitespaces to keep the length standard
                String tmp = Utils.leftpad(key, 48);

                byte[] term = ByteBuffer.allocate(48).put(tmp.getBytes()).array();
                byte[] offsetDocId = ByteBuffer.allocate(4).putInt(termInfo.getOffsetDocId()).array();
                byte[] offsetFrequency = ByteBuffer.allocate(4).putInt(termInfo.getOffsetFrequency()).array();
                byte[] postingListLength = ByteBuffer.allocate(4).putInt(termInfo.getPostingListLength()).array();

                try {
                    randomAccessFile.write(term);
                    randomAccessFile.write(offsetDocId);
                    randomAccessFile.write(offsetFrequency);
                    randomAccessFile.write(postingListLength);

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

            });
        }catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Reads the given lexicon block into a TreeMap
     * @param inputPath path of the file that contains the block's lexicon
     */
    public TreeMap<String, TermInfo> readLexiconFromFile(String inputPath){

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(inputPath, "rw")){

            TreeMap<String, TermInfo> lexiconTmp = new TreeMap<>();
            byte[] b;
            String term;
            int offsetDocId;
            int offsetFreq;
            int lexiconLength;

            long length = randomAccessFile.length()/(60);
            System.out.println("LENGTH: " + length);

            for(int i = 0; i < length; i ++){
                b = new byte[48];
                randomAccessFile.seek(i* 60L);
                randomAccessFile.readFully(b, 0, 48);
                term = new String(b, Charset.defaultCharset()).trim();
                offsetDocId = randomAccessFile.readInt();
                offsetFreq = randomAccessFile.readInt();
                lexiconLength = randomAccessFile.readInt();

                lexiconTmp.put(term, new TermInfo(offsetDocId, offsetFreq, lexiconLength));
            }

            return lexiconTmp;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }



    /**
     * Writes the current inverted index in the disk, the inverted index is written in two different files:
     * The file containing the document ids of each posting list
     * The file containing the frequencies of the terms in the documents
     * Then update the information in the lexicon
     * @param outputPathDocIds path of the file that will contain the document ids
     * @param outputPathFrequencies path of the file that will contain the frequencies
     */
    public void writeInvertedIndexToFile(String outputPathDocIds, String outputPathFrequencies){

        //Create resources with try-catch with resources
        try (RandomAccessFile docIdBlock = new RandomAccessFile(outputPathDocIds, "rw");
             RandomAccessFile frequencyBlock = new RandomAccessFile(outputPathFrequencies, "rw"))
        {

            AtomicInteger currentOffset = new AtomicInteger(0);

            //for each element of the inverted index
            invertedIndex.forEach((term, postingList) -> {

                //Set the current offsets to be written in the lexicon
                int offsetDocId = currentOffset.get();
                int offsetFrequency = currentOffset.get();

                postingList.forEach(posting -> {
                    //Create the buffers for each element to be written
                    byte[] postingDocId = ByteBuffer.allocate(4).putInt(posting.getDoc_id()).array();
                    byte[] postingFreq = ByteBuffer.allocate(4).putInt(posting.getFrequency()).array();

                    try {
                        //Append each element to the file, each one adds 4 bytes to the file
                        docIdBlock.write(postingDocId);
                        frequencyBlock.write(postingFreq);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    //Increment the current offset
                    currentOffset.addAndGet(4);
                });

                //Set the docId offset, the frequency offset, the posting list length of the term in the lexicon
                lexicon.get(term).set(offsetDocId, offsetFrequency, postingList.size());

            });
        }catch (IOException e) {
            System.err.println("Exception during file creation of block");
            throw new RuntimeException(e);
        }
    }

    public HashMap<String, TermInfo> getLexicon() {
        return lexicon;
    }

    public HashMap<String, ArrayList<Posting>> getInvertedIndex() {
        return invertedIndex;
    }


    public static void main(String[] args){
        /*InvertedIndexBuilder indexBuilder = new InvertedIndexBuilder();
        indexBuilder.insertDocument(new ParsedDocument(1,new String[]{"d","b","g","r","a","p","a"},"21"));
        indexBuilder.insertDocument(new ParsedDocument(3,new String[]{"h","b","t","b","1","u","b"},"e21"));
        System.out.println(indexBuilder.invertedIndex);

        System.out.println("Non sorted:"+indexBuilder.lexicon);
        indexBuilder.sortLexicon();
        System.out.println("Sorted:" + indexBuilder.lexicon);
        indexBuilder.writeInvertedIndexToFile("src/main/resources/files/provaDocId.txt", "src/main/resources/files/provaFreq.txt");
        indexBuilder.writeLexiconToFile("src/main/resources/files/provaWrite.txt");
        System.out.println("\n\n WRITTEN \n\n");
        System.out.println(indexBuilder.readLexiconFromFile("src/main/resources/files/provaWrite.txt"));*/
    }



}