import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

public class Utility {

    public static void generateData() throws IOException {
        
        final int numberOfRecords = 100;
        char[] charArray = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
        String cwd = System.getProperty("user.dir");
        FileOutputStream file = null;

        Random rand = new Random();
        ArrayList<String> records = new ArrayList<>();
    
        for (int i = 0; i < numberOfRecords; i++) {
            String record = "";
            int transactionId = i+1;
            int transactionSaleAmount = rand.nextInt(numberOfRecords)+1;
            String customerName = ""+charArray[rand.nextInt(26)]+charArray[rand.nextInt(26)]+charArray[rand.nextInt(26)];
            int categoryOfItem = rand.nextInt(1500)+1;
            record += transactionId+","+transactionSaleAmount+","+customerName+","+categoryOfItem+"\n";

            // if (i < numberOfRecords-1) record += "\n";

            records.add(record);
        }

        Collections.shuffle(records);

        try {
            file = new FileOutputStream(cwd+"/dataset.txt");
        } catch (FileNotFoundException e) {
            //file already exists
        }
        for (String record : records) {
            file.write(record.getBytes());
        }
        file.close();;
    }

    public static String getHashValue(String record) {
        
        // replace %7 to %16 for hash
        return String.format("%7s", Integer.toBinaryString(Integer.parseInt(record.split(",")[1]))).replace(" ", "0");
    }
    
}
