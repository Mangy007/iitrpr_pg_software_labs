import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class ExtendibleHash {

    static String cwd = System.getProperty("user.dir");
    static Bucket[] secondaryMemory = new Bucket[100000];
    static Map<String, Integer> bucketAddressTable = new HashMap<String, Integer>() {};
    
    public static void main(String args[]) {
        
        try {
            Utility.generateData();
            File file = new File(cwd+"/dataset.txt");
            Scanner fileReader = new Scanner(file);
            int bucketIndexInSecondaryMemory = 0;
            int globalDepth = 0;
            while(fileReader.hasNextLine()) {
                String record = fileReader.nextLine();
                String hashValue = Utility.getHashValue(record);
                String key = hashValue.subSequence(0, globalDepth).toString();
                // if bucket is full create new bucket and increase global depth
                if(globalDepth==0) {
                    if(bucketAddressTable.isEmpty()) {
                        //create a bucket and insert a record to it
                        Bucket bucket = new Bucket();
                        bucket.addRecord(record);
                        secondaryMemory[bucketIndexInSecondaryMemory] = bucket;
                        bucketAddressTable.put(key, bucketIndexInSecondaryMemory++);
                    }
                    else {
                        int secondaryMemoryIndex = bucketAddressTable.get(key);
                        secondaryMemory[secondaryMemoryIndex].addRecord(record);
                    }
                }
            }
            fileReader.close();

            for (Bucket bucket : secondaryMemory) {
                if(bucket!=null) {
                    System.out.println(bucket);
                }
            }

        } catch (Exception e) {
            // do nothing
        }
    }
}
