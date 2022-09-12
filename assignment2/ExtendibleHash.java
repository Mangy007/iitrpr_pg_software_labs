import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class ExtendibleHash {

    static String cwd = System.getProperty("user.dir");
    static Map<String, Integer> bucketAddressTable = new HashMap<String, Integer>() {};
    static final int hashLength = 16;
    static final int numberOfRecords = 30;
    // static final int bucketSize = 2;
    
    public static void main(String args[]) {
        
        try {
            Utility.generateData(numberOfRecords);
            File file = new File(cwd+"/dataset.txt");
            Scanner fileReader = new Scanner(file);
            String outpuString = "";
            Scanner scn = new Scanner(System.in);
            int bucketSize = scn.nextInt();
            Bucket.setBucketSize(bucketSize);
            Utility.setHashLength(hashLength);
            int globalDepth = 0;
            while(fileReader.hasNextLine()) {
                String strRecord = fileReader.nextLine();
                Record record = new Record(strRecord);
                String hashValue = Utility.getHashValue(record);
                String hashPrefix = hashValue.substring(0, globalDepth); 
                // String key = hashValue.substring(0, globalDepth); // redundant variable, can be replaced with hashPrefix
                if(globalDepth == 0) {
                    if(bucketAddressTable.isEmpty()) {
                        //create a bucket and insert a record to it
                        Bucket bucket = SimulatedSecondaryMemory.getNewBucket();
                        bucket.addRecord(record);
                        bucketAddressTable.put(hashPrefix, SimulatedSecondaryMemory.lastFilledBuckedIndex);
                    }
                    else {
                        int secondaryMemoryIndex = bucketAddressTable.get(hashPrefix);
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
                    int secondaryMemoryIndex = bucketAddressTable.get(hashPrefix);
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

                        Utility.performBucketExpansion(bucketAddressTable, currBucket);

                        Utility.addRecordToBucket(record, globalDepth, bucketAddressTable, hashPrefix, currBucket);

                    }
                    else if(initialBucket.localDepth == globalDepth) {
                        // perform table expansion and rehashing if bucket is full
                        globalDepth++;
                        if(globalDepth > hashLength) break;
                        bucketAddressTable =  Utility.performRehashingAndTableExpansion(bucketAddressTable, record, globalDepth);

                    }
                }
                // outpuString += Utility.printHashTable(bucketAddressTable, globalDepth);
                // outpuString += "\n######################################################\n";
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
