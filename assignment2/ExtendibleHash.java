import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class ExtendibleHash {

    static String cwd = System.getProperty("user.dir");
    static Map<String, Integer> bucketAddressTable = new HashMap<String, Integer>() {};
    static final int numberOfRecords = 20;
    static final int bucketSize = 5;
    static final int hashLength = 16;
    
    public static void main(String args[]) {
        
        try {
            // Utility.generateData(numberOfRecords);
            File file = new File(cwd+"/dataset.txt");
            Scanner fileReader = new Scanner(file);
            String outpuString = "";

            Bucket.setBucketSize(bucketSize);
            Utility.setHashLength(hashLength);
            int globalDepth = 0;
            while(fileReader.hasNextLine()) {
                String strRecord = fileReader.nextLine();
                Record record = new Record(strRecord);
                String hashValue = Utility.getHashValue(record);
                String hashPrefix = hashValue.substring(0, globalDepth); 
                String key = hashValue.substring(0, globalDepth); // redundant variable, can be replaced with hashPrefix
                if(globalDepth == 0) {
                    if(bucketAddressTable.isEmpty()) {
                        //create a bucket and insert a record to it
                        Bucket bucket = SimulatedSecondaryMemory.getNewBucket();
                        bucket.addRecord(record);
                        bucketAddressTable.put(key, SimulatedSecondaryMemory.lastFilledBuckedIndex);
                    }
                    else {
                        int secondaryMemoryIndex = bucketAddressTable.get(key);
                        Bucket initialBucket = SimulatedSecondaryMemory.getBucket(secondaryMemoryIndex);
                        if(!initialBucket.isBucketFull()) {
                            // insert record as bucket has some space left for new records
                            initialBucket.addRecord(record);
                        }
                        else {
                            // perform table expansion and rehashing
                            globalDepth++;
                            bucketAddressTable =  Utility.performRehashingAndTableExpansion(bucketAddressTable, record, globalDepth);
                        }
                    }
                }
                else {
                    int secondaryMemoryIndex = bucketAddressTable.get(key);
                    Bucket initialBucket = SimulatedSecondaryMemory.getBucket(secondaryMemoryIndex);

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
                        // create bucket and share bucket with same key prefix based on local depth

                        String prevKeyPrefix = "";
                        String lastMatchedKey = "";
                        List<String> bucketAddressTableKeyset = new ArrayList<String>(bucketAddressTable.keySet());
                        Collections.sort(bucketAddressTableKeyset);
                        for (String currKey : bucketAddressTableKeyset) {
                            if(currBucket == SimulatedSecondaryMemory.getBucket(bucketAddressTable.get(currKey))) {
                                String currKeyPrefix = currKey.substring(0, currBucket.localDepth);
                                if(!currKeyPrefix.equals(prevKeyPrefix)) {
                                    prevKeyPrefix = currKeyPrefix;
                                    lastMatchedKey = currKey;
                                    Bucket newBucket = SimulatedSecondaryMemory.getNewBucket();
                                    newBucket.localDepth = currBucket.localDepth;
                                    bucketAddressTable.put(currKey, SimulatedSecondaryMemory.lastFilledBuckedIndex);
                                }
                                else {
                                    bucketAddressTable.put(currKey, bucketAddressTable.get(lastMatchedKey));
                                }
                            }
                        }

                        // perform chaining if bucket is full
                        while(currBucket!=null) {
                            for (Record currRecord : currBucket.records) {
                                String prevRecordHashValue = Utility.getHashValue(currRecord);
                                String prevRecordHashPrefix = prevRecordHashValue.substring(0, globalDepth);
                                Bucket bucket = SimulatedSecondaryMemory.getBucket(bucketAddressTable.get(prevRecordHashPrefix));
                                if(bucket.isBucketFull()) {
                                    while(bucket!=null) {
                                        if(bucket.nextBucket==null) break;
                                        bucket = bucket.nextBucket;
                                    }
                                    if(bucket.isBucketFull()) {
                                        Bucket newBucket = SimulatedSecondaryMemory.getNewBucket();
                                        newBucket.addRecord(currRecord);
                                        newBucket.localDepth = bucket.localDepth;
                                        bucket.nextBucket = newBucket;
                                    }
                                    else {
                                        bucket.addRecord(currRecord);
                                    }
                                }
                                else {
                                    bucket.addRecord(currRecord);
                                }
                            }
                            SimulatedSecondaryMemory.removeBucket(currBucket);
                            currBucket = currBucket.nextBucket;
                        }
                        // add new record
                        initialBucket = SimulatedSecondaryMemory.getBucket(bucketAddressTable.get(hashPrefix));
                        while(initialBucket!=null) {
                            if(initialBucket.nextBucket==null) break;
                            initialBucket = initialBucket.nextBucket;
                        }
                        if(initialBucket.isBucketFull()) {
                            Bucket newBucket = SimulatedSecondaryMemory.getNewBucket();
                            newBucket.addRecord(record);
                            newBucket.localDepth = initialBucket.localDepth;
                            initialBucket.nextBucket = newBucket;
                        }
                        else {
                            initialBucket.addRecord(record);
                        }
                    }
                    else if(initialBucket.localDepth == globalDepth) {
                        // perform table expansion and rehashing if bucket is full
                        globalDepth++;
                        if(globalDepth > hashLength) break;
                        bucketAddressTable =  Utility.performRehashingAndTableExpansion(bucketAddressTable, record, globalDepth);

                    }
                }
                outpuString += Utility.printHashTable(bucketAddressTable, globalDepth);
                outpuString += "\n######################################################\n";
            }
            fileReader.close();

            outpuString += "\t\tFINAL OUTPUT";
            outpuString += "\n######################################################\n\n";
            outpuString += Utility.printHashTable(bucketAddressTable, globalDepth);

            Utility.createFile(outpuString, "output");

        } catch (Exception e) {
            // do nothing
            e.printStackTrace();
        }
    }
}
