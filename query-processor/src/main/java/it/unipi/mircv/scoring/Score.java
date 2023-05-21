package it.unipi.mircv.scoring;

import it.unipi.mircv.beans.*;

import java.util.ArrayList;
import java.util.PriorityQueue;

public class Score {

    //Values used for the BM25 scoring
    static final double K1 = 1.5;
    static final double B = 0.75;
    
    static final int BEST_K_VALUE = 20;

    //Retrieve the statistics of the inverted index
    static final Statistics statistics = new Statistics();

    /**
     * Implementation of the algorithm Document-At-a-Time, it iterates over all the posting lists accessing and scoring
     * the document with the minimum document id.
     * @param postingLists Array of posting lists.
     * @param documentIndex document index containing the information of the documents.
     * @param BM25 if it is true, then the BM25 scoring is applied, otherwise the scoring is TFIDF.
     * @return an ordered array of tuples containing the document id and the score associated with the document.
     */
    public static ArrayList<Tuple<Long,Double>> scoreCollectionDisjunctive(PostingList[] postingLists, DocumentIndex documentIndex, boolean BM25) {

        RankedDocs rankedDocs = new RankedDocs(BEST_K_VALUE);
        ArrayList<Integer> nonEssential = new ArrayList<>();
        ArrayList<Integer> essential = new ArrayList<>();

        ArrayList<PostingList> orderedPostingLists = new ArrayList<>();
        //Retrieve the time at the beginning of the computation
        long begin = System.currentTimeMillis();

        //Move the iterators of each posting list to the first position
        for (PostingList postingList : postingLists) {
            if (postingList.hasNext()) {
                postingList.next();
                orderedPostingLists.add(postingList);
                //essential.add(postingList.getTermInfo());
            }
        }

        if(BM25) {
            orderedPostingLists.sort((o1, o2) -> Integer.compare(o2.getTermInfo().getBm25TermUpperBound(), o1.getTermInfo().getBm25TermUpperBound()));
        }
        else{
            orderedPostingLists.sort((o1, o2) -> Integer.compare(o2.getTermInfo().getTfidfTermUpperBound(), o1.getTermInfo().getTfidfTermUpperBound()));
        }
        
        for(int i = 0; i < orderedPostingLists.size(); i++){
            essential.add(i);
        }

        //Tuple to store the current minimum document id and the list of posting lists containing it
        Tuple<Long,ArrayList<Integer>> minDocidTuple;

        //Support variables to accumulate over the iteration the score values
        double tf_tfidf;
        double tf_BM25;
        double score = 0;

        //Access each posting list in a Document-At-a-Time fashion until no more postings are available
        while (!allPostingListsEnded(postingLists)) {

            //Retrieve the minimum document id and the list of posting lists containing it
            minDocidTuple = minDocid(postingLists);

            //Debug
            //System.out.println("------------------");
            //System.out.println("Min docID: " + minDocidTuple.getFirst());
            //System.out.println("Blocks with minDocID: " + minDocidTuple.getSecond());

            //For each index in the list of posting lists with min doc id
            for(Integer index : minDocidTuple.getSecond()) {

                //If the scoring is BM25
                if(BM25){
                    // TODO: 21/05/2023 implement maxScore for BM25 
                    //Compute the BM25's tf for the current posting
                    tf_BM25 = postingLists[index].getFreq()/ (K1 * ((1-B) + B * ( (double)documentIndex.get(postingLists[index].getDocId()).getDocLength() / statistics.getAvdl()) + postingLists[index].getFreq()));

                    //Add the partial score to the accumulated score
                    score += tf_BM25*postingLists[index].getTermInfo().getIdf();

                }else {
                    // TODO: 21/05/2023 implement maxScore for tfIdf 
                    //Compute the TFIDF'S tf for the current posting
                    tf_tfidf = 1 + Math.log(postingLists[index].getFreq()) / Math.log(2);

                    //Add the partial score to the accumulated score
                    score += tf_tfidf*postingLists[index].getTermInfo().getIdf();
                }

                //Move the cursor to the next posting, if there is one, otherwise the flag of the posting list is set to
                // true, in this way we mark the end of the posting list
                postingLists[index].next();

            }

            //Debug
            //System.out.println("------------------");
            //System.out.println("Docid:" + minDocidTuple.getFirst());
            //System.out.println("tfidf: " + tf_tfidf);
            //System.out.println("BM25: " + tf_BM25);
            //System.out.println("------------------");


            //Add the score of the current document to the priority queue
            if(score > rankedDocs.getThreshold()){
                rankedDocs.add(new Tuple<>(minDocidTuple.getFirst(), score));
                // TODO: 21/05/2023 finish essential and non essential lists 
            }

            //Clear the support variables for the next iteration
            score = 0;
        }

        //Print the time used to score the documents, so to generate an answer for the query
        System.out.println("[SCORE DOCUMENT] Total scoring time: " + (System.currentTimeMillis() - begin) + "ms");

        return getBestKDocuments(rankedDocs, BEST_K_VALUE);
    }



    public static ArrayList<Tuple<Long,Double>> scoreCollectionConjunctive(PostingList[] postingLists, DocumentIndex documentIndex, boolean BM25) {

        //Priority queue to store the document id and its score, based on the priority of the document
        PriorityQueue<Tuple<Long,Double>> rankedDocs = new PriorityQueue<>((o1, o2) -> o2.getSecond().compareTo(o1.getSecond()));

        //Retrieve the time at the beginning of the computation
        long begin = System.currentTimeMillis();

        //Move the iterators of each posting list to the first position
        for (PostingList postingList : postingLists) {
            if (postingList.hasNext()) {
                System.out.println(postingList.next());
            }
        }

        //Tuple to store the current minimum document id and the list of posting lists containing it
        long maxDocid;

        //Support variables to accumulate over the iteration the score values
        double tf_tfidf;
        double tf_BM25;
        double score = 0;

        //Access each posting list in a Document-At-a-Time fashion until no more postings are available
        while (!aPostingListEnded(postingLists)) {

            //Retrieve the minimum document id and the list of posting lists containing it
            maxDocid = maxDocid(postingLists);

            //Perform the nextGEQ operation for each posting list
            for(PostingList postingList : postingLists){

                //If we reach the end of the posting list then we break the for, the conjunctive query is ended
                // and all the next conditions are not satisfied
                if(postingList.nextGEQ(maxDocid) == null || postingList.noMorePostings())
                   break;
            }

            //If the current doc id is equal in all the posting lists
            if(areAllEqual(postingLists)){

                //Debug
                System.out.println("--------------------------------");

                //Score the document
                for(PostingList postingList : postingLists) {

                    //Debug
                    System.out.println(postingList.getDocId());

                    //If the scoring is BM25
                    if (BM25) {

                        //Compute the BM25's tf for the current posting
                        tf_BM25 = postingList.getFreq() / (K1 * ((1 - B) + B * ((double) documentIndex.get(postingList.getDocId()).getDocLength() / statistics.getAvdl()) + postingList.getFreq()));

                        //Add the partial score to the accumulated score
                        score += tf_BM25 * postingList.getTermInfo().getIdf();

                    } else {

                        //Compute the TFIDF'S tf for the current posting
                        tf_tfidf = 1 + Math.log(postingList.getFreq()) / Math.log(2);

                        //Add the partial score to the accumulated score
                        score += tf_tfidf * postingList.getTermInfo().getIdf();
                    }

                    //Move the cursor to the next posting (this must be done even if the document is not scored), if there
                    // is one, otherwise set the flag of the posting list to true, in this way we mark the end of the
                    // posting list
                    if(postingList.hasNext()){

                        //Move the cursor to the next posting
                        postingList.next();
                    }else {

                        //Debug
                        System.out.println("no more postings");

                        //Set the noMorePostings flag of the posting list to true
                        postingList.setNoMorePostings();
                    }

                }

                //Debug
                System.out.println("--------------------------------");

                //Since we have a document in all the posting lists then its score is relevant for the conjunctive query
                // it's value must be added to the priority queue, otherwise the score is not relevant, and we don't add it.
                //Add the score of the current document to the priority queue
                rankedDocs.add(new Tuple<>(maxDocid, score));
            }
            
            //Clear the support variables for the next iteration
            score = 0;
        }

        //Print the time used to score the documents, so to generate an answer for the query
        System.out.println("[SCORE DOCUMENT] Total scoring time: " + (System.currentTimeMillis() - begin) + "ms");

        //Return the top k documents
        return getBestKDocuments(rankedDocs, BEST_K_VALUE);
    }

    /**
     * Check if all the current doc ids of each posting list are equal.
     * @param postingLists array of posting lists to check
     * @return true if all the current doc ids are equal, false otherwise
     */
    private static boolean areAllEqual(PostingList[] postingLists){

        long docid = -1;

        //Traverse all the posting lists if two different docids are found, then return false
        for(PostingList postingList : postingLists){

            //If at least one is ended
            if(postingList == null){
                return false;
            }

            if(docid == -1){
                docid = postingList.getDocId();
            }else if(docid != postingList.getDocId()){
                return false;
            }
        }

        //All the docids are equal
        return true;
    }

    /**
     * Checks if all the posting lists are ended, so the iterator has reached the end of each posting list.
     * @param postingLists Array of posting lists.
     * @return true if all the posting lists are ended (no more postings), false otherwise.
     */
    public static boolean allPostingListsEnded(PostingList[] postingLists){

        //For each posting list check if it has a next posting
        for (PostingList postingList : postingLists) {

            //If at least one posting is available return false
            if (!postingList.noMorePostings()) {
                return false;
            }
        }

        //If all the posting lists are traversed then return false
        return true;
    }

    public static boolean aPostingListEnded(PostingList[] postingLists){

        //For each posting list check if it has a next posting
        for (PostingList postingList : postingLists) {

            //If at least one posting is ended return true
            if (postingList.noMorePostings()) {
                return true;
            }
        }

        //If all the posting lists are traversed then return false
        return false;
    }



    /**
     * Extract the first k tuples (docID, score) from the priority queue, in descending order of score.
     * @param rankedDocs The priority queue containing the documents and their scores.
     * @param k The number of tuples to extract.
     * @return an ordered array of tuples containing the document id and the score associated with the document.
     */
    public static ArrayList<Tuple<Long, Double>> getBestKDocuments(PriorityQueue<Tuple<Long,Double>> rankedDocs,int k){

        //Array list used to build the result
        ArrayList<Tuple<Long, Double>> results = new ArrayList<>();

        //Tuple used to contain the current (docID, score) tuple
        Tuple<Long,Double> tuple;

        //Until k tuples are polled from the priority queue
        while(results.size() < k){

            //Retrieve the first tuple from the priority queue based on the score value (descending order)
            tuple = rankedDocs.poll();

            //If the tuple is null then we've reached the end of the priority queue, less than k tuples were present
            if(tuple == null){
                break;
            }

            //Add the tuple to the result list
            results.add(tuple);
        }

        //Return the result list
        return results;
    }

    /**
     * Compute the minimum document id in a collection of posting lists, then populates an arrayList containing
     * the indices of the postingList array with the minimum document id. Returns a tuple containing the minimum
     * doc id and the array of indices.
     * We exploit the fact that the posting lists are sorted by document id, so we can optimize the search accessing
     * only the current term pointed by the iterator of each posting list.
     * @param postingLists Array of posting lists.
     * @return a tuple containing the minimum doc id and the array of indices of the postingList array with min doc id.
     */
    public static Tuple<Long, ArrayList<Integer>>  minDocid(PostingList[] postingLists) {

        //Variable to store the minimum document id
        long minDocid = Long.MAX_VALUE;

        //Array to store the posting lists with the minimum document id
        ArrayList<Integer> postingListsWithMinDocid = new ArrayList<>();

        //Retrieve the minimum document id, we've just to check the first element of each posting list since we have
        //the document id in the posting lists ordered by increasing document id value
        //For each posting list we check the current document id
        for(int i = 0; i < postingLists.length; i++){

            //Skip the i-th posting list if the list don't contain more postings, we've reached the end of the list
            if(postingLists[i].noMorePostings()){
                continue;
            }

            //If the current docid is smaller than the minDocId, then update the minDocId.
            if (postingLists[i].getDocId() < minDocid) {

                //If we've found a new minimum document id, store it
                minDocid = postingLists[i].getDocId();

                //Clear the list of posting lists with the minimum document id
                postingListsWithMinDocid.clear();

                //Add the current posting list to the list of posting lists with the minimum document id
                postingListsWithMinDocid.add(i);

                //Else if the current docid is equal to the min term, then add the current posting list
                // to the list of posting lists with the min docid.
            }else if (postingLists[i].getDocId() == minDocid) {

                //Add the current posting list to the list of posting lists with the min docid
                postingListsWithMinDocid.add(i);
            }
        }

        //Return the minimum document id and the list of posting lists with the minimum document id
        return new Tuple<>(minDocid, postingListsWithMinDocid);
    }


    /**
     * Get the maximum document id from the passed posting list array
     * @param postingLists posting list from which analyze the current docid to retrieve the maximum
     * @return the maximum document id
     */
    private static long maxDocid(PostingList[] postingLists){

        long max = -1;

        //Traverse the array of posting list and find the maximum document id among the current doc ids
        for(PostingList postingList : postingLists){
            if(postingList.getDocId() > max){
                max = postingList.getDocId();
            }
        }
        return max;
    }

    //Main for testing it the methods works -> all methods are tested!
    public static void main( String[] args )
    {

    }

}
