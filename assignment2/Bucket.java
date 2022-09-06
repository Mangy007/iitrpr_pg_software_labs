public class Bucket {
    
    int localDepth = 0;
    static int bucketSize = 0;
    int numberOfEmptySpaces = 0;
    Record[] records = null;
    Bucket nextBucket = null;

    int recordIndex = 0;
    
    // public Bucket(int bucketSize) {
        
    //     this.numberOfEmptySpaces = bucketSize;
    //     this.records = new Record[bucketSize];
    // }
    
    public Bucket() {
        
        this.numberOfEmptySpaces = bucketSize;
        this.records = new Record[bucketSize];
    }

    public static void setBucketSize(int size) {
        
        bucketSize = size;
    }

    public void addRecord(String record) {
        
        if(this.recordIndex >= bucketSize) {
            // System.err.println("Bucket full\n");
            return;
        }
        records[this.recordIndex++] = new Record(record);
        this.numberOfEmptySpaces--;
    }

    public void addRecord(Record record) {
        
        if(this.numberOfEmptySpaces==0) {
            // bucket full
            // System.err.println("Bucket full\n");
            return;
        }
        records[this.recordIndex++] = record;
        this.numberOfEmptySpaces--;
    }

    public int size() {
        
        return recordIndex;
    }

    public boolean isBucketFull() {
        
        if(this.numberOfEmptySpaces == 0) return true;
        return false;
    }

    public boolean isBucketEmpty() {
        
        if(this.numberOfEmptySpaces > 0) return true;
        return false;
    }
    
    @Override
    public String toString() {
        
        String result = "";

        for (Record record : records) {
            if(record != null)
                result += record+"\n";
        }
        
        Bucket tempBucket = this.nextBucket;
        while(tempBucket != null) {
            result += "\n";
            for (Record record : tempBucket.records) {
                if(record != null)
                    result += record+"\n";
            }
            tempBucket = tempBucket.nextBucket;
        }

        return result;
    }
}
