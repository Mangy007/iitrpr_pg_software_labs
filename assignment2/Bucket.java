public class Bucket {
    
    int localDepth = 0;
    static int bucketSize = 0;
    int numberOfEmptySpaces = 0;
    Record[] records = null;
    Bucket nextBucket = null;
    int bucketIndexInSecondaryMemory = -1;

    int recordIndex = 0;
    
    public Bucket() {
        
        this.numberOfEmptySpaces = bucketSize;
        this.records = new Record[bucketSize];
    }

    public static void setBucketSize(int size) {
        
        bucketSize = size;
    }

    public void addRecord(Record record) {
        
        if(this.numberOfEmptySpaces==0) {
            // bucket full
            return;
        }
        records[this.recordIndex++] = record;
        this.numberOfEmptySpaces--;
    }

    public boolean isBucketFull() {
        
        if(this.numberOfEmptySpaces == 0) return true;
        return false;
    }

    public boolean isBucketEmpty() {
        
        if(this.numberOfEmptySpaces > 0) return true;
        return false;
    }

    public String getChainedIndexesInSecondaryMemory() {

        String result = ""+this.bucketIndexInSecondaryMemory;
        Bucket tempBucket = this.nextBucket;

        while(tempBucket != null) {
            result += ","+tempBucket.bucketIndexInSecondaryMemory;
            tempBucket = tempBucket.nextBucket;
        }

        return result;
    }
    
    @Override
    public String toString() {
        
        String result = "";

        for (Record record : records) {
            if(record != null)
                result += "\t"+record+"\n";
        }
        
        Bucket tempBucket = this.nextBucket;
        while(tempBucket != null) {
            result += "\n";
            for (Record record : tempBucket.records) {
                if(record != null)
                    result += "\t"+record+"\n";
            }
            tempBucket = tempBucket.nextBucket;
        }

        return result;
    }

}
