public class Record {
    
    int transactionId = 0;
    int transactionSaleAmount =0;
    String customerName = null;
    int categoryOfItem = 0;
    
    
    public Record(int transactionId, int transactionSaleAmount, String customerName, int categoryOfItem) {
        
        this.transactionId = transactionId;
        this.transactionSaleAmount = transactionSaleAmount;
        this.customerName = customerName;
        this.categoryOfItem = categoryOfItem;
    }

    public Record(String record) {
        
        this.transactionId = Integer.parseInt(record.split(",")[0]);
        this.transactionSaleAmount = Integer.parseInt(record.split(",")[1]);
        this.customerName = record.split(",")[2];
        this.categoryOfItem = Integer.parseInt(record.split(",")[3]);
    }

    @Override
    public String toString() {
        
        String result = "";

        result += transactionId+","+transactionSaleAmount+","+customerName+","+categoryOfItem;
        return result;
    }
    
}
