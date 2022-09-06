public class SimulatedSecondaryMemory {
    
    Bucket[] simulatedSecondaryMemory = new Bucket[100000];
    int lastFilledBuckedIndex = -1;

    public void addBucket(Bucket bucket) {
        
        this.simulatedSecondaryMemory[++this.lastFilledBuckedIndex] = bucket;
    }

    public Bucket getBucket(int index) {
        
        return this.simulatedSecondaryMemory[index];
    }

    public Bucket getLastInsertedBucket() {
        
        return this.simulatedSecondaryMemory[this.lastFilledBuckedIndex];
    }
}
