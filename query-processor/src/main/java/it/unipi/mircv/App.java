package it.unipi.mircv;

import it.unipi.mircv.beans.*;

import java.util.ArrayList;

public class App 
{
    public static void main( String[] args )
    {
        /*DocumentIndex documentIndex = new DocumentIndex();
        documentIndex.loadDocumentIndex();
        System.out.println(documentIndex.size());*/

        Lexicon lexicon = new Lexicon();
        lexicon.loadLexicon();
        System.out.println(lexicon.size());

        System.out.println(lexicon.get("artificial"));
        System.out.println(lexicon.get("dog"));
        System.out.println(lexicon.get("sport"));

        String[] words = {"artificial", "dog", "sport"};

        PostingList[] postingList = new PostingList[words.length];

        for (int i = 0; i < words.length; i++){
            postingList[i] = new PostingList();
            postingList[i].openList(lexicon.get(words[i]));
        }

        System.out.println(minDocid(postingList));

        String query = "Artificial intelligence news";
        // TODO: 04/04/2023 parse the query

        // TODO: 04/04/2023 load the postings

        //todo score with daat

        //todo results
    }

    /**
     * Implementation of the algorithm Document-At-a-Time, it iterates over all the posting lists accessing and scoring
     * the document with the minimum document id.
     * @param postingLists Array of posting lists
     * @return an ordered array of tuples containing the document id and the score associated with the document.
     */
    public static ArrayList<Tuple<Long,Integer>> scoreCollection(PostingList[] postingLists){

        ArrayList<Integer> postingListsWithMinDocid = new ArrayList<>();
        ArrayList<Long> rankedDocuments = new ArrayList<>();

        //Move the iterator to the first position
        for (PostingList postings : postingLists) {
            if (postings.hasNext()) {
                postings.next();
            }
        }

        //DAAT
        while (allPostingsEnded(postingLists)) {
            // TODO: 05/04/2023 retrieve min docid and list of postings

            // TODO: 05/04/2023 Access each list, compute the score and move each posting to the next position
        }

        // TODO: 05/04/2023 Sort the documents based on their score

        // TODO: 05/04/2023 Return the sorted documents
        return null;
    }

    /**
     * Checks if all the posting lists are ended, so the iterator has reached the end of each posting list.
     * @param postingLists Array of posting lists.
     * @return true if all the posting lists are ended (no more postings), false otherwise.
     */
    public static boolean allPostingsEnded(PostingList[] postingLists){

        //For each posting list check if it has a next posting
        for (PostingList postingList : postingLists) {
            if (!postingList.hasNext()) {
                return false;
            }
        }
        return true;
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

}
