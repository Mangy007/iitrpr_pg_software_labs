public class Bucket {
    
    int localDepth = 0;
    int bucketSize = 0;
    int numberOfEmptySpaces = 0;
    Record[] records = null;
    Bucket nextBucket = null;

    int recordIndex = 0;
    
    public Bucket(int bucketSize) {
        
        this.bucketSize = bucketSize;
        this.records = new Record[this.bucketSize];
    }
    
    public Bucket() {
        
        this.bucketSize = 5;
        this.records = new Record[this.bucketSize];
    }

    public void addRecord(String record) {
        
        if(this.recordIndex >= bucketSize) {
            // System.err.println("Bucket full\n");
            return;
        }
        records[this.recordIndex++] = new Record(record);
    }

    public int size() {
        
        return recordIndex;
    }
    
    @Override
    public String toString() {
        
        String result = "";

        for (Record record : records) {
            result += record+"\n";
        }

        return result;
    }
}
