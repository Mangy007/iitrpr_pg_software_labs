public class SimulatedSecondaryMemory {
    
    static Bucket[] simulatedSecondaryMemory = new Bucket[100000];
    static int lastFilledBuckedIndex = -1;

    static {

        for (int i = 0; i < simulatedSecondaryMemory.length; i++) {
            simulatedSecondaryMemory[i] = new Bucket();
        }
    }

    public static Bucket getNewBucket() {
        lastFilledBuckedIndex += 1;
        simulatedSecondaryMemory[lastFilledBuckedIndex].bucketIndexInSecondaryMemory = lastFilledBuckedIndex;
        return simulatedSecondaryMemory[lastFilledBuckedIndex];
    }

    // public static void addBucket(Bucket bucket) {
        
    //     bucket.bucketIndexInSecondaryMemory = lastFilledBuckedIndex+1;
    //     simulatedSecondaryMemory[++lastFilledBuckedIndex] = bucket;
    // }

    public static Bucket getBucket(int index) {
        
        return simulatedSecondaryMemory[index];
    }

    public static void removeBucket(Bucket bucket) {
        
        simulatedSecondaryMemory[bucket.bucketIndexInSecondaryMemory] = null;
    }

    // public static Bucket getLastInsertedBucket() {
        
    //     return simulatedSecondaryMemory[lastFilledBuckedIndex];
    // }
}
