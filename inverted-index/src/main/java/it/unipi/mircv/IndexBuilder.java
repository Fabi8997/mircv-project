package it.unipi.mircv;

import java.util.*;
import java.util.stream.Stream;

// TODO: 27/10/2022 CHECK IF THE SPIMI ALGORITHM IS CORRECTLY IMPLEMENTED

/**
 * Represent a component that gives the methods to build the lexicon and the inverted index for each block.
 */
public class IndexBuilder {
    HashMap<String, Integer> lexicon;
    HashMap<Integer, ArrayList<Posting>> invertedIndex;
    int currTermID;

    /**
     * Constructor of the class.
     * Instantiate the HashMap for the lexicon and the inverted index, used for the fast lookup that requires O(1);
     * Set the first termID to 1, the term id for each block carries also the information about the position of a term
     * in the inverted index.
     */
    public IndexBuilder() {
        lexicon = new HashMap<>();
        invertedIndex = new HashMap<>();
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
     * @param processedDocument String containing a document id and the list of tokens of that specific document.
     *                          The required format is [docId]\t[listOfTokens], the [] are not present in the string.
     *                          Example: 232    this is an example
     */
    public void insertDocument(String processedDocument) {

        //Integer to keep the document id of the given document
        int docId;

        //String containing the tokenized text
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

        //Split the text using the whitespace as delimiter, generate a stream of String
        Stream.of(text.split(" "))
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
                            if(p.getDoc_id() == docId){

                                //Increment the frequency of the doc id
                                p.frequency++;

                                found = true;
                                break;
                            }
                        }

                        //If the posting of the document is not present in the posting list, it must be added
                        if(!found){

                            //Posting added to the posting list of the term
                            termPostingList.add(new Posting(docId, 1));
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
                        // TODO: 27/10/2022 Change into an array of int (???)
                        ArrayList<Posting> postingsList = new ArrayList<>();
                        Posting posting = new Posting(docId, 1);
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
        clearTermId();
    }


    /**
     * Writes the current lexicon into a file
     */
    public void writeLexiconToFile(){
        // TODO: 27/10/2022 Implement the write of the lexicon in the disk
    }

    /**
     * Writes the current inverted index in the disk, the inverted index is written in two different files:
     * The file containing the document ids of each posting list
     * The file containing the frequencies of the terms in the documents
     */
    public void writeInvertedIndexToFile(){
        // TODO: 27/10/2022 Implement the write of the inverted index's files
    }

    /**
     * Convert the lexicon into a string organized in lines, where each line contains the term and the associated term
     * id, in each block the term id corresponds to the posting list line that contains the term's posting list.
     * @return Return the lexicon as a string.
     */
    public String getLexicon(){

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
        IndexBuilder indexBuilder = new IndexBuilder();
        /*indexBuilder.insertDocument("1\tProva prova ciao");
        indexBuilder.insertDocument("2\tProva posting prova ciao");
        indexBuilder.insertDocument("3\tProva addios prova ciao");*/
        System.out.println(indexBuilder.invertedIndex);
        System.out.println(indexBuilder.getInvertedIndex()[0] + "\n\n" + indexBuilder.getInvertedIndex()[1]);
    }
}
