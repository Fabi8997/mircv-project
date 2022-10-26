package it.unipi.mircv;

import java.util.*;

public class IndexBuilder {
    HashMap<String, Integer> lexicon;
    HashMap<Integer, ArrayList<Posting>> invertedIndex;

    public IndexBuilder() {
        lexicon = new HashMap<>();
        invertedIndex = new HashMap<>();
    }

    public void createLexiconAndPostingList(String processedDocument) {
        String[] components = processedDocument.split("\\s+");
        int docId = Integer.parseInt(components[0]);
        int currTermID = 0;
        for(int i = 1; i < components.length; i++){
            if(lexicon.containsValue(components[i])){
                //update posting list
                ArrayList<Posting> termPostingList = invertedIndex.get(lexicon.get(components[i]));
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
                lexicon.put(components[i], currTermID);

                //create a new posting list in the inverted index
                invertedIndex.put(currTermID, new ArrayList<>(Arrays.asList(new Posting(docId, 1))));

                currTermID++;
            }
        }
    }
}
