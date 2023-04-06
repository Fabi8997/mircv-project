package it.unipi.mircv.scoring;

import it.unipi.mircv.beans.PostingList;
import it.unipi.mircv.beans.Tuple;

import java.util.ArrayList;
import java.util.PriorityQueue;

public class Score {

    /**
     * Implementation of the algorithm Document-At-a-Time, it iterates over all the posting lists accessing and scoring
     * the document with the minimum document id.
     * @param postingLists Array of posting lists
     * @return an ordered array of tuples containing the document id and the score associated with the document.
     */
    public static ArrayList<Tuple<Long,Double>> scoreCollection(PostingList[] postingLists){

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
        Tuple<Long,ArrayList<Integer>> minDocidTuple;

        //FOR DEBUG -> this will be the double score
        int sum = 0;

        // TODO: 05/04/2023 Access each list, compute the score and move each posting to the next position
        //Access each posting list in a Document-At-a-Time fashion until no more postings are available
        while (!allPostingsEnded(postingLists)) {

            //Retrieve the minimum document id and the list of posting lists containing it
            minDocidTuple = minDocid(postingLists);

            //Debug
            System.out.println("------------------");
            System.out.println("Min docID: " + minDocidTuple.getFirst());
            System.out.println("Blocks with minDocID: " + minDocidTuple.getSecond());

            //For each index in the list of posting lists with min doc id
            for(Integer index : minDocidTuple.getSecond()) {

                //Debug sum the frequency of the term in each document with docid equal to min doc id
                sum += postingLists[index].getFreq();


                //Move the cursor to the next posting, if there is one, otherwise set the flag of the posting list to
                // true, in this way we mark the end of the posting list
                if(postingLists[index].hasNext()){

                    //Move the cursor to the next posting
                    postingLists[index].next();
                }else {

                    //Debug
                    System.out.println("no more postings");

                    //Set the noMorePostings flag of the posting list to true
                    postingLists[index].setNoMorePostings();
                }
            }

            //Debug
            System.out.println("Sum: " + sum);
            System.out.println("------------------");

            // TODO: 06/04/2023 Change the sum with the score
            //Add the score of the current document to the priority queue
            rankedDocs.add(new Tuple<>(minDocidTuple.getFirst(), (double) sum));

            //Reset the support variables for the next iteration
            sum = 0;
        }

        //Print the time used to score the documents, so to generate an answer for the query
        System.out.println("[SCORE DOCUMENT] Total scoring time: " + (System.currentTimeMillis() - begin) + "ms");

        return getBestKDocuments(rankedDocs, 20);
    }

    /**
     * Checks if all the posting lists are ended, so the iterator has reached the end of each posting list.
     * @param postingLists Array of posting lists.
     * @return true if all the posting lists are ended (no more postings), false otherwise.
     */
    public static boolean allPostingsEnded(PostingList[] postingLists){

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

    //Main for testing it the methods works -> all methods are tested!
    public static void main( String[] args )
    {

    }

}
