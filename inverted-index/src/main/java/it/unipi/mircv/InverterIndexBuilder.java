package it.unipi.mircv;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

// TODO: 27/10/2022 CHECK IF THE SPIMI ALGORITHM IS CORRECTLY IMPLEMENTED

// TODO: 03/11/2022 ADD the document index part, we can generate it also in this part! 

/**
 * Represent a component that gives the methods to build the lexicon and the inverted index for each block.
 */
public class InverterIndexBuilder {
    HashMap<String, Integer> lexicon;
    HashMap<Integer, ArrayList<Posting>> invertedIndex;
    HashMap<Integer, Integer> documentIndex;

    //static DB documentIndexDb = DBMaker.fileDB(DOCUMENT_INDEX_DB_PATH).make();

    /*static HTreeMap<Integer, byte[]> documentIndex = documentIndexDb.hashMap("document_index")
            .keySerializer(Serializer.INTEGER)
            .valueSerializer(Serializer.BYTE_ARRAY)
            .create();
    */

    int currTermID;

    /**
     * Constructor of the class.
     * Instantiate the HashMap for the lexicon and the inverted index, used for the fast lookup that requires O(1);
     * Set the first termID to 1, the term id for each block carries also the information about the position of a term
     * in the inverted index.
     */
    public InverterIndexBuilder() {
        lexicon = new HashMap<>();
        invertedIndex = new HashMap<>();
        documentIndex = new HashMap<>();
        currTermID = 1;
    }

    public InverterIndexBuilder(int blockNumber) {
        lexicon = new HashMap<>();
        invertedIndex = new HashMap<>();
        documentIndex = new HashMap<>();
        currTermID = 1;
    }

    /*public void insertDocument2(String processedDocument) {
        int docId;
        String text;

        //Divide the line using \t as delimiter, it'll split the doc_id and the text
        StringTokenizer stringTokenizer = new StringTokenizer(processedDocument, "\t");

        //Retrieve the first token, that is the docno
        if(stringTokenizer.hasMoreTokens()){
            docId = Integer.parseInt(stringTokenizer.nextToken());

            //Retrieve the second token, that is the text and cast it to lower case
            if(stringTokenizer.hasMoreTokens()){
                text = stringTokenizer.nextToken().toLowerCase();
            }else{
                //The text is empty, or it was not possible to retrieve it
                return;
            }
        }else{
            //The line is empty, or it was not possible to retrieve it
            return;
        }

        String[] words = text.split(" ");
        for(int i = 0; i < words.length; i++){
            if(lexicon.containsKey(words[i])){

                //update posting list
                ArrayList<Posting> termPostingList = invertedIndex.get(lexicon.get(words[i]));
                boolean found = false;
                for(Posting p : termPostingList){
                    if(p.getDoc_id() == docId){
                        //posting relative to docID already present -> must be updated
                        p.frequency++;
                        found = true;
                    }
                }
                if(!found){
                    //posting relative to docId not present -> must be added
                    termPostingList.add(new Posting(docId, 1));
                }
            }
            else{
                //create a new element in lexicon
                lexicon.put(words[i], currTermID);

                //create a new posting list in the inverted index
                ArrayList<Posting> postingsList = new ArrayList<>();
                Posting posting = new Posting(docId, 1);
                postingsList.add(posting);
                invertedIndex.put(currTermID, postingsList);

                currTermID++;
            }
        }
    }*/

    /**
     * Insert the document's tokens inside the lexicon and the inverted index
     * @param parsedDocument Contains the id of the document, its length and the list of tokens
     */
    public void insertDocument(ParsedDocument parsedDocument) {

        //Insert the information of the document in the document index for the block
        documentIndex.put(parsedDocument.docId, parsedDocument.documentLength);

        //Generate a stream of String
        Stream.of(parsedDocument.terms)
                .forEach((term) -> {
                    //If the term is already present in the lexicon
                    if(lexicon.containsKey(term)){

                        //Retrieve the posting list of the term
                        ArrayList<Posting> termPostingList = invertedIndex.get(lexicon.get(term));

                        //Flag to set if the doc id's posting is present in the posting list of the term
                        boolean found = false;

                        //Iterate through the posting
                        for(Posting p : termPostingList){

                            //If the doc id is present, increment the frequency and terminate the loop
                            if(p.getDoc_id() == parsedDocument.docId){

                                //Increment the frequency of the doc id
                                p.frequency++;

                                found = true;
                                break;
                            }
                        }

                        //If the posting of the document is not present in the posting list, it must be added
                        if(!found){

                            //Posting added to the posting list of the term
                            termPostingList.add(new Posting(parsedDocument.docId, 1));
                        }
                    }
                    //If the term was not present in the lexicon
                    else{
                        //Insert a new element in the lexicon, in each block the currTermID corresponds to the id
                        // associated to the term, but also to the position in the inverted index!
                        // To access the posting list of that term we can just retrieve the currTermId and access the
                        // array of posting lists
                        lexicon.put(term, currTermID);

                        //Insert a new posting list in the inverted index
                        ArrayList<Posting> postingsList = new ArrayList<>();
                        Posting posting = new Posting(parsedDocument.docId, 1);
                        postingsList.add(posting);
                        invertedIndex.put(currTermID, postingsList);

                        currTermID++;
                    }
                });
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
     * Clear the instance of the document index, it must be used after the document index has been written in the disk.
     */
    private void clearDocumentIndex(){
        documentIndex.clear();
    }

    /**
     * Reset the current term id, it must be used before starting to process a new block.
     */
    private void clearTermId(){
        currTermID = 1;
    }

    /**
     * Clear the class instances in order to be used for a new block processing.
     */
    public void clear(){
        clearLexicon();
        clearInvertedIndex();
        clearDocumentIndex();
        clearTermId();

    }


    /**
     * Writes the current lexicon into a file
     * @param outputPath path of the file that will contain the block's lexicon
     */
    public void writeLexiconToFile(String outputPath){

        // TODO: 22/12/2022 Scrivere il lexicon in un random access file
    }

    public void writeDocumentIndexToFile(String outputPath){

        // TODO: 22/12/2022 Scrivere il doc index su random access file o map db
    }

    /**
     * Writes the current inverted index in the disk, the inverted index is written in two different files:
     * The file containing the document ids of each posting list
     * The file containing the frequencies of the terms in the documents
     * @param outputPathDocIds path of the file that will contain the document ids
     * @param outputPathFrequencies path of the file that will contain the frequencies
     */
    public void writeInvertedIndexToFile(String outputPathDocIds, String outputPathFrequencies){
        RandomAccessFile docIdBlock = null, freqBlock = null;
        try{
            //document instantiation
            docIdBlock = new RandomAccessFile(outputPathDocIds, "rw");
            freqBlock = new RandomAccessFile(outputPathDocIds, "rw");
        }
        catch(FileNotFoundException fnfEx){
            System.err.println("Exception during file creation of block");
            fnfEx.printStackTrace();
        }
        RandomAccessFile finalDocIdBlock = docIdBlock;
        RandomAccessFile finalFreqBlock = freqBlock;
        AtomicInteger currentOffset = new AtomicInteger();

        invertedIndex.forEach((termId, postingList) -> {    //for each element of the inverted index
            int arrayLength = (postingList.size() + 1) * 4; //byte length of the information of the posting list
            byte[] serializedIds = new byte[arrayLength];   //int for the term id + int for every docId
            byte[] serializedFreqs = new byte[arrayLength]; //int for the term id + int for every frequency
            byte[] serializedTermId = ByteBuffer.allocate(4).putInt(termId).array();

            // TODO: 22/12/2022 controllare se serve il termId
            System.arraycopy(serializedTermId, 0, serializedIds, 0, 4);
            System.arraycopy(serializedTermId, 0, serializedFreqs, 0, 4);

            AtomicInteger pos = new AtomicInteger(1);
            postingList.forEach(posting -> {
                //copy of the byte representation of the single posting info
                System.arraycopy(ByteBuffer.allocate(4).putInt(posting.doc_id).array(), 0, serializedIds, pos.get() * 4, 4);
                System.arraycopy(ByteBuffer.allocate(4).putInt(posting.frequency).array(), 0, serializedFreqs, pos.get() * 4, 4);
                pos.getAndIncrement();
            });

            try{
                //write the document information to disk
                finalDocIdBlock.write(serializedIds, currentOffset.get(), arrayLength);
                finalFreqBlock.write(serializedFreqs, currentOffset.get(), arrayLength);
                // TODO: 22/12/2022 aggiorna offset in lexicon

                currentOffset.addAndGet(arrayLength);
            }
            catch(IOException ioEx) {
                System.err.println("Exception during write");
                ioEx.printStackTrace();
            }
        });
    }

    /**
     * Convert the lexicon into a string organized in lines, where each line contains the term and the associated term
     * id, in each block the term id corresponds to the posting list line that contains the term's posting list.
     * @return Return the lexicon as a string.
     */
    public String getLexicon(){

        // TODO: 22/12/2022 conversione in byte
        //Object used to build the lexicon into a string
        StringBuilder stringBuilder = new StringBuilder();

        //Iterate over the lexicon
        lexicon.forEach((k,v) ->

                //For each key-value pair generate the line
                stringBuilder.append(k).append("\t").append(v).append("\n")
        );

        return  stringBuilder.toString();
    }

    /**
     * Convert the inverted index into two strings, each one composed by lines. Each line contains, respectively, the
     * list of doc ids in the first string, and the list of frequencies in the second string. Each line is associated
     * to a term.
     * @return Return an array of String: the first contains the inverted index's doc ids file, the second contains the
     * inverted index's frequencies file.
     */
    public String[] getInvertedIndex(){

        // TODO: 22/12/2022 conversione in byte
        //Object used to build the list of doc ids from a posting list
        StringBuilder stringBuilderDocIds = new StringBuilder();

        //Object used to build the list of frequencies from a posting list
        StringBuilder stringBuilderFrequencies = new StringBuilder();

        //Iterate over the inverted index
        invertedIndex.forEach((k,v) -> {

            //For each key-value pair, where K = termID and V = termId's posting list, are retrieved the list of doc
            // ids and the list of frequencies as strings.
            String[] postingList = getPostingList(v);

            //The first element of the postingList array is the whitespace separated list of doc ids, it is appended
            // to the string buffer with a newline at the end.
            stringBuilderDocIds.append(postingList[0]).append("\n");

            //The second element of the postingList array is the whitespace separated list of frequencies, it is
            // appended to the string buffer with a newline at the end.
            stringBuilderFrequencies.append(postingList[1]).append("\n");
        });

        return new String[]{ stringBuilderDocIds.toString(), stringBuilderFrequencies.toString()};
    }

    /**
     * Given a posting list, it extracts the list of doc ids and the list of frequencies and put them into two strings.
     * @param postingList Array list containing the posting list of a term
     * @return Return an array of String: the first contains a list of doc ids separated by a whitespace, the second
     * contains a list of frequencies separated by a whitespace.
     */
    private String[] getPostingList(ArrayList<Posting> postingList){

        // TODO: 22/12/2022 conversione posting list

        //Object used to build the list of doc ids into a string
        StringBuilder stringBuilderDocIds = new StringBuilder();

        //Object used to build the list of frequencies into a string
        StringBuilder stringBuilderFrequencies = new StringBuilder();

        //Iterate over the array list
        for(Posting posting: postingList){

            //For each element append the doc id and the frequency to their respective string builders
            stringBuilderDocIds.append(" ").append(posting.getDoc_id());
            stringBuilderFrequencies.append(" ").append(posting.getFrequency());
        }

        return new String[]{ stringBuilderDocIds.toString(), stringBuilderFrequencies.toString()};
    }

    public static void main(String[] args){
        /*IndexBuilder indexBuilder = new IndexBuilder();
        System.out.println(indexBuilder.invertedIndex);
        System.out.println(indexBuilder.getInvertedIndex()[0] + "\n\n" + indexBuilder.getInvertedIndex()[1]);*/
    }
}
