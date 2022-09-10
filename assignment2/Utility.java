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

    static int hashLength = 0;      // hashlength/number_of_bits is set to 0

    public static void generateData(int numberOfRecords) throws IOException {
        
        char[] charArray = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
        String cwd = System.getProperty("user.dir");
        FileOutputStream file = null;

        Random rand = new Random();
        // ArrayList<String> records = new ArrayList<>();
        String records = "";
    
        for (int i = 0; i < numberOfRecords; i++) {
            int transactionId = (i*2011);
            int transactionSaleAmount = rand.nextInt(numberOfRecords)+1;
            String customerName = ""+charArray[rand.nextInt(26)]+charArray[rand.nextInt(26)]+charArray[rand.nextInt(26)];
            int categoryOfItem = rand.nextInt(1500)+1;
            records += transactionId+","+transactionSaleAmount+","+customerName+","+categoryOfItem+"\n";

            // if (i < numberOfRecords-1) record += "\n";

            // records.add(record);
        }

        // Collections.shuffle(records);

        createFile(records, "dataset");
    }

    public static void setHashLength(int value) {
        
        hashLength = value;
    }

    public static String getHashValue(Record record) {
        
        // replace %7 to %16 for hash
        String format = "%"+hashLength+"s";
        return String.format(format, Integer.toBinaryString(record.transactionId)).replace(" ", "0");
    }

    public static Map<String, Integer> performRehashingAndTableExpansion(Map<String, Integer> bucketAddressTable, Record newRecord, int globalDepth) {
    
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
        Bucket initialBucket = SimulatedSecondaryMemory.getBucket(tempBucketAddressTable.get(hashPrefix));

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

            String prevKeyPrefix = "";
            String lastMatchedKey = "";
            List<String> bucketAddressTableKeyset = new ArrayList<String>(tempBucketAddressTable.keySet());
            Collections.sort(bucketAddressTableKeyset);
            for (String currKey : bucketAddressTableKeyset) {
                if(currBucket == SimulatedSecondaryMemory.getBucket(tempBucketAddressTable.get(currKey))) {
                    if(currKey.equals("")) {
                        Bucket newBucket = SimulatedSecondaryMemory.getNewBucket();
                        newBucket.localDepth = currBucket.localDepth;
                        tempBucketAddressTable.put(currKey, SimulatedSecondaryMemory.lastFilledBuckedIndex);
                    }
                    else {
                        String currKeyPrefix = currKey.substring(0, currBucket.localDepth);
                        if(!currKeyPrefix.equals(prevKeyPrefix)) {
                            prevKeyPrefix = currKeyPrefix;
                            lastMatchedKey = currKey;
                            Bucket newBucket = SimulatedSecondaryMemory.getNewBucket();
                            newBucket.localDepth = currBucket.localDepth;
                            tempBucketAddressTable.put(currKey, SimulatedSecondaryMemory.lastFilledBuckedIndex);
                        }
                        else {
                            tempBucketAddressTable.put(currKey, tempBucketAddressTable.get(lastMatchedKey));
                        }
                    }
                }
            }
            
            // perform chaining if bucket is full
            while(currBucket!=null) {
                for (Record currRecord : currBucket.records) {
                    String prevRecordHashValue = Utility.getHashValue(currRecord);
                    String prevRecordHashPrefix = prevRecordHashValue.substring(0, globalDepth);
                    Bucket bucket = SimulatedSecondaryMemory.getBucket(tempBucketAddressTable.get(prevRecordHashPrefix));
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
            initialBucket = SimulatedSecondaryMemory.getBucket(tempBucketAddressTable.get(hashPrefix));
            while(initialBucket!=null) {
                if(initialBucket.nextBucket==null) break;
                initialBucket = initialBucket.nextBucket;
            }
            if(initialBucket.isBucketFull()) {
                Bucket newBucket = SimulatedSecondaryMemory.getNewBucket();
                newBucket.addRecord(newRecord);
                newBucket.localDepth = initialBucket.localDepth;
                initialBucket.nextBucket = newBucket;
            }
            else {
                initialBucket.addRecord(newRecord);
            }
        }


        return tempBucketAddressTable;
    }

    public static String printHashTable(Map<String, Integer> bucketAddressTable, int globalDepth) {

        List<String> list = new ArrayList<String>(bucketAddressTable.keySet());
        Collections.sort(list);
        String output = "";
        
        for (String key : list) {
            int bucketIndex = bucketAddressTable.get(key);
            Bucket bucket = SimulatedSecondaryMemory.getBucket(bucketIndex);
            String allChainedBucketIndexes = bucket.getChainedIndexesInSecondaryMemory();
            output += "Local Depth: "+bucket.localDepth+", Bucket No: "+allChainedBucketIndexes+"\n";
            output += bucket;
            output += "\n";
        }
        output += "\nBucket Address Table:";
        output += "\n---------------------";
        output += "\n\tGlobal Depth: "+globalDepth+"\n\n";
        for (String key : list) {
            int bucketIndex = bucketAddressTable.get(key);
            Bucket bucket = SimulatedSecondaryMemory.getBucket(bucketIndex);
            String bucketNumbers = ""+bucket.bucketIndexInSecondaryMemory;
            bucket = bucket.nextBucket;
            while(bucket != null) {
                bucketNumbers += "->"+bucket.bucketIndexInSecondaryMemory;
                bucket = bucket.nextBucket;
            }
            output += key+" : "+bucketNumbers+"\n";
        }

        return output;
    }

    public static void createFile(String input, String filename) throws IOException {

        String cwd = System.getProperty("user.dir");
        FileOutputStream fileWrite = null;
        fileWrite = new FileOutputStream(cwd+"/"+filename+".txt");
        fileWrite.write(input.getBytes());
        fileWrite.close();
    }
    
}
