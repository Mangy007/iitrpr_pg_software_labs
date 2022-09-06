import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class ExtendibleHash {

    static String cwd = System.getProperty("user.dir");
    static final int numberOfRecords = 15;
    static Map<String, Integer> bucketAddressTable = new HashMap<String, Integer>() {};

    static SimulatedSecondaryMemory secondaryMemory = new SimulatedSecondaryMemory();
    
    public static void main(String args[]) {
        
        try {
            // Utility.generateData(numberOfRecords);
            File file = new File(cwd+"/dataset.txt");
            Scanner fileReader = new Scanner(file);

            Bucket.setBucketSize(2);
            Utility.setHashLength(4);
            int globalDepth = 0;
            while(fileReader.hasNextLine()) {
                String strRecord = fileReader.nextLine();
                Record record = new Record(strRecord);
                String hashValue = Utility.getHashValue(record);
                String hashPrefix = hashValue.substring(0, globalDepth);
                String key = hashValue.substring(0, globalDepth);
                if(globalDepth == 0) {
                    if(bucketAddressTable.isEmpty()) {
                        //create a bucket and insert a record to it
                        Bucket bucket = new Bucket();
                        bucket.addRecord(record);
                        secondaryMemory.addBucket(bucket);
                        bucketAddressTable.put(key, secondaryMemory.lastFilledBuckedIndex);
                    }
                    else {
                        int secondaryMemoryIndex = bucketAddressTable.get(key);
                        Bucket initialBucket = secondaryMemory.getBucket(secondaryMemoryIndex);
                        if(!initialBucket.isBucketFull()) {
                            // insert record as bucket has some space left for new records
                            initialBucket.addRecord(record);
                        }
                        else {
                            // perform table expansion and rehashing
                            globalDepth++;
                            bucketAddressTable =  Utility.performRehashingAndTableExpansion(bucketAddressTable, secondaryMemory, record, globalDepth);
                        }
                    }
                }
                else {
                    int secondaryMemoryIndex = bucketAddressTable.get(key);
                    Bucket initialBucket = secondaryMemory.getBucket(secondaryMemoryIndex);

                    Bucket currBucket = initialBucket;
                    while(currBucket!=null) {
                        if(currBucket.nextBucket == null) break;
                        currBucket = currBucket.nextBucket;
                    }
                    if(!currBucket.isBucketFull()) {
                        // insert record as bucket has some space left for new records
                        currBucket.addRecord(record);
                    }
                    else if(initialBucket.localDepth < globalDepth) {
                        // perform bucket expansion as local depth < global depth
                        currBucket = initialBucket;
                        currBucket.localDepth++;
                        for (String currKey : bucketAddressTable.keySet()) {
                            if(currBucket == secondaryMemory.getBucket(bucketAddressTable.get(currKey))) {
                                Bucket newBucket = new Bucket();
                                newBucket.localDepth = currBucket.localDepth;
                                secondaryMemory.addBucket(newBucket);
                                bucketAddressTable.put(currKey, secondaryMemory.lastFilledBuckedIndex);
                            }
                        }
                        // perform chaining if bucket is full
                        while(currBucket!=null) {
                            for (Record currRecord : currBucket.records) {
                                String prevRecordHashValue = Utility.getHashValue(currRecord);
                                String prevRecordHashPrefix = prevRecordHashValue.substring(0, globalDepth);
                                Bucket bucket = secondaryMemory.getBucket(bucketAddressTable.get(prevRecordHashPrefix));
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
                        initialBucket = secondaryMemory.getBucket(bucketAddressTable.get(hashPrefix));
                        while(initialBucket!=null) {
                            if(initialBucket.nextBucket==null) break;
                            initialBucket = initialBucket.nextBucket;
                        }
                        if(initialBucket.isBucketFull()) {
                            Bucket newBucket = new Bucket();
                            newBucket.addRecord(record);
                            newBucket.localDepth = initialBucket.localDepth;
                            initialBucket.nextBucket = newBucket;
                            secondaryMemory.addBucket(newBucket);
                        }
                        else {
                            initialBucket.addRecord(record);
                        }
                    }
                    else if(initialBucket.localDepth == globalDepth) {
                        // perform table expansion and rehashing if bucket is full
                        globalDepth++;
                        bucketAddressTable =  Utility.performRehashingAndTableExpansion(bucketAddressTable, secondaryMemory, record, globalDepth);

                    }
                }
            }
            fileReader.close();

            Utility.printHashTable(bucketAddressTable, secondaryMemory ,globalDepth);

        } catch (Exception e) {
            // do nothing
            e.printStackTrace();
        }
    }
}
