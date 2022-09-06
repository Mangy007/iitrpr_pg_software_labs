import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class Utility {

    static int hashLength = 0;

    public static void generateData(int numberOfRecords) throws IOException {
        
        char[] charArray = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
        String cwd = System.getProperty("user.dir");
        FileOutputStream file = null;

        Random rand = new Random();
        ArrayList<String> records = new ArrayList<>();
    
        for (int i = 0; i < numberOfRecords; i++) {
            String record = "";
            int transactionId = i+1;
            int transactionSaleAmount = rand.nextInt(numberOfRecords)+1;
            String customerName = ""+charArray[rand.nextInt(26)]+charArray[rand.nextInt(26)]+charArray[rand.nextInt(26)];
            int categoryOfItem = rand.nextInt(1500)+1;
            record += transactionId+","+transactionSaleAmount+","+customerName+","+categoryOfItem+"\n";

            // if (i < numberOfRecords-1) record += "\n";

            records.add(record);
        }

        // Collections.shuffle(records);

        try {
            file = new FileOutputStream(cwd+"/dataset.txt");
        } catch (FileNotFoundException e) {
            //file already exists
        }
        for (String record : records) {
            file.write(record.getBytes());
        }
        file.close();;
    }

    public static void setHashLength(int value) {
        
        hashLength = value;
    }

    public static String getHashValue(Record record) {
        
        // replace %7 to %16 for hash
        String format = "%"+hashLength+"s";
        return String.format(format, Integer.toBinaryString(record.transactionId)).replace(" ", "0");
    }

    public static Map<String, Integer> performRehashingAndTableExpansion(Map<String, Integer> bucketAddressTable,
                                                        SimulatedSecondaryMemory secondaryMemory, Record newRecord, int globalDepth) {
    
        Map<String, Integer> tempBucketAddressTable = new HashMap<>();
        int tableSize = (int) Math.pow(2, globalDepth);

        // create new bucket address table based on global depth
        for (int i = 0; i < tableSize; i++) {
            String format = "%"+globalDepth+"s";
            String key = String.format(format, Integer.toBinaryString(i)).replace(" ", "0");
            int bucketIndexInSecondaryMemory;
            if(globalDepth==1)
                bucketIndexInSecondaryMemory = bucketAddressTable.get("");
            else
                bucketIndexInSecondaryMemory = bucketAddressTable.get(key.substring(0, globalDepth-1));

            tempBucketAddressTable.put(key, bucketIndexInSecondaryMemory);
        }

        String hashValue = Utility.getHashValue(newRecord);
        String hashPrefix = hashValue.substring(0, globalDepth);
        Bucket initialBucket = secondaryMemory.getBucket(tempBucketAddressTable.get(hashPrefix));

        Bucket currBucket = initialBucket;
        while(currBucket!=null) {
            if(currBucket.nextBucket == null) break;
            currBucket = currBucket.nextBucket;
        }
        if(!currBucket.isBucketFull()) {
            // insert record as bucket / bucket chain has some space left for new records
            currBucket.addRecord(newRecord);
        }
        else {
            // perform bucket expansion as local depth < global depth
            currBucket = initialBucket;
            currBucket.localDepth++;
            for (String currKey : tempBucketAddressTable.keySet()) {
                if(currBucket == secondaryMemory.getBucket(tempBucketAddressTable.get(currKey))) {
                    Bucket newBucket = new Bucket();
                    newBucket.localDepth = currBucket.localDepth;
                    secondaryMemory.addBucket(newBucket);
                    tempBucketAddressTable.put(currKey, secondaryMemory.lastFilledBuckedIndex);
                }
            }
            // perform chaining if bucket is full
            while(currBucket!=null) {
                for (Record currRecord : currBucket.records) {
                    String prevRecordHashValue = Utility.getHashValue(currRecord);
                    String prevRecordHashPrefix = prevRecordHashValue.substring(0, globalDepth);
                    Bucket bucket = secondaryMemory.getBucket(tempBucketAddressTable.get(prevRecordHashPrefix));
                    if(bucket.isBucketFull()) {
                        while(bucket!=null) {
                            if(bucket.nextBucket==null) break;
                            bucket = bucket.nextBucket;
                        }
                        if(bucket.isBucketFull()) {
                            Bucket newBucket = new Bucket();
                            newBucket.addRecord(currRecord);
                            newBucket.localDepth = bucket.localDepth;
                            bucket.nextBucket = newBucket;
                            secondaryMemory.addBucket(newBucket);
                        }
                        else {
                            bucket.addRecord(currRecord);
                        }
                    }
                    else {
                        bucket.addRecord(currRecord);
                    }
                }
                currBucket = currBucket.nextBucket;
            }
            // add new record
            initialBucket = secondaryMemory.getBucket(tempBucketAddressTable.get(hashPrefix));
            while(initialBucket!=null) {
                if(initialBucket.nextBucket==null) break;
                initialBucket = initialBucket.nextBucket;
            }
            if(initialBucket.isBucketFull()) {
                Bucket newBucket = new Bucket();
                newBucket.addRecord(newRecord);
                newBucket.localDepth = initialBucket.localDepth;
                initialBucket.nextBucket = newBucket;
                secondaryMemory.addBucket(newBucket);
            }
            else {
                initialBucket.addRecord(newRecord);
            }
        }


        return tempBucketAddressTable;
    }

	public static Map<String, Integer> performRehashing(Map<String, Integer> bucketAddressTable,
			                                                SimulatedSecondaryMemory secondaryMemory, Record newRecord, int globalDepth) {

        Map<String, Integer> tempBucketAddressTable = new HashMap<>();
        int tableSize = (int) Math.pow(2, globalDepth);

        for (int i = 0; i < tableSize; i++) {
            String format = "%"+globalDepth+"s";
            String key = String.format(format, Integer.toBinaryString(i)).replace(" ", "0");
            // enter keys to tempBucketAddressTable and use value of bucketAddressTable based on prefix match using tempGlobalDepth
            int bucketIndexInSecondaryMemory;
            if(globalDepth==1)
                bucketIndexInSecondaryMemory = bucketAddressTable.get("");
            else
                bucketIndexInSecondaryMemory = bucketAddressTable.get(key.substring(0, globalDepth-1));

            tempBucketAddressTable.put(key, bucketIndexInSecondaryMemory);
        }

        String hashValue = Utility.getHashValue(newRecord);
        String hashPrefix = hashValue.substring(0, globalDepth);
        Bucket prevBucket = secondaryMemory.getBucket(tempBucketAddressTable.get(hashPrefix));
        if(prevBucket.isBucketFull()) {
            if(prevBucket.localDepth < globalDepth) {
                // perform bucket expansion
                prevBucket.localDepth++;

                for (String key : tempBucketAddressTable.keySet()) {
                    if(prevBucket == secondaryMemory.getBucket(tempBucketAddressTable.get(key))) {
                        Bucket newBucket = new Bucket();
                        newBucket.localDepth = prevBucket.localDepth;
                        secondaryMemory.addBucket(newBucket);
                        tempBucketAddressTable.put(key, secondaryMemory.lastFilledBuckedIndex);
                    }
                }

                // iterate chain and perform rehashing

                // while(prevBucket!=null) {
                //     System.out.println(prevBucket);
                //     for (Record record : prevBucket.records) {
                //         String prevRecordHashValue = Utility.getHashValue(record);
                //         String prevRecordHashPrefix = prevRecordHashValue.substring(0, globalDepth);
                //         Bucket currBucket = secondaryMemory.getBucket(tempBucketAddressTable.get(prevRecordHashPrefix));
                //         currBucket.addRecord(record);
                //     }
                //     prevBucket = prevBucket.nextBucket;
                // }

                for (Record record : prevBucket.records) {
                    String prevRecordHashValue = Utility.getHashValue(record);
                    String prevRecordHashPrefix = prevRecordHashValue.substring(0, globalDepth);
                    Bucket currBucket = secondaryMemory.getBucket(tempBucketAddressTable.get(prevRecordHashPrefix));
                    currBucket.addRecord(record);
                }

                // String prevRecordHashValue = Utility.getHashValue(newRecord);
                // String prevRecordHashPrefix = prevRecordHashValue.substring(0, globalDepth);
                Bucket currBucket = secondaryMemory.getBucket(tempBucketAddressTable.get(hashPrefix));
                // currBucket.addRecord(newRecord);
                while(currBucket!=null && currBucket.isBucketFull()) {
                    // iterate bucket chain
                    if(currBucket.nextBucket == null) break;
                    else currBucket = currBucket.nextBucket;
                }
                if(currBucket.isBucketEmpty()) {
                    // insert record to chained bucket
                    currBucket.addRecord(newRecord);
                }
                else {
                    // chain new bucket
                    Bucket chainBucket = new Bucket();
                    chainBucket.addRecord(newRecord);
                    chainBucket.localDepth = currBucket.localDepth;
                    secondaryMemory.addBucket(chainBucket);
                    currBucket.nextBucket = chainBucket;
                }

                // for (String key : tempBucketAddressTable.keySet()) {
                //     if(key.startsWith(hashPrefix)) {
                //         Bucket currBucket = secondaryMemory.getBucket(tempBucketAddressTable.get(key));
                //         for (Record record : prevBucket.records) {
                //             String prevRecordHashValue = Utility.getHashValue(record);
                //             if(prevRecordHashValue.startsWith(key))
                //                 currBucket.addRecord(record);
                //         }
                //     }
                // }
            }
            else {
                // perform rehashing
                // globalDepth++;
                // bucketAddressTable =  Utility.performRehashing(bucketAddressTable, secondaryMemory, newRecord, globalDepth);
            }
        }
        else {
            prevBucket.addRecord(newRecord);
        }

		return tempBucketAddressTable;
	}

    public static void printHashTable(Map<String, Integer> bucketAddressTable, SimulatedSecondaryMemory secondaryMemory, int globalDepth) {

        List<String> list = new ArrayList<String>(bucketAddressTable.keySet());
        Collections.sort(list);
        System.out.println();
        for (String key : list) {
            int bucketIndex = bucketAddressTable.get(key);
            Bucket bucket = secondaryMemory.getBucket(bucketIndex);
            System.out.println("Local Depth: "+bucket.localDepth+" B_"+bucketIndex+" ***");
            System.out.println(bucket);
            System.out.println("*************************");
        }
        System.out.println("\n ************ Bucket Address Table:*********");
        System.out.println("             Global Depth: "+globalDepth+"\n");
        for (String key : list) {
            int bucketIndex = bucketAddressTable.get(key);
            System.out.println(key+" : "+bucketIndex);
        }
    }
    
}
