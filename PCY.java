import java.io.*;
import java.util.*;

public class PCY {
    public static void main(String[] args) throws Exception {
        String filePath = "Assignment-1_Data.csv";
        double minSupport = 0.01;  // minimum support threshold (1%)

        // Step 1: Read dataset and build transactions
        Map<String, List<String>> transactions = new LinkedHashMap<>();
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        String line;
        br.readLine(); // skip header
        while ((line = br.readLine()) != null) {
            String[] parts = line.split(";");
            if (parts.length < 2) continue;
            String billNo = parts[0].trim();
            String item = parts[1].trim();
            transactions.computeIfAbsent(billNo, k -> new ArrayList<>()).add(item);
        }
        br.close();

        int numTransactions = transactions.size();
        int minCount = (int) Math.ceil(minSupport * numTransactions);
        System.out.println("Total Transactions: " + numTransactions);
        System.out.println("Min Support Count: " + minCount);

        // Step 2: First Pass — Count single items
        Map<String, Integer> itemCount = new HashMap<>();
        for (List<String> items : transactions.values()) {
            Set<String> unique = new HashSet<>(items);
            for (String item : unique) {
                itemCount.put(item, itemCount.getOrDefault(item, 0) + 1);
            }
        }

        // Step 3: Keep only frequent items
        Set<String> frequentItems = new HashSet<>();
        for (Map.Entry<String, Integer> e : itemCount.entrySet()) {
            if (e.getValue() >= minCount) {
                frequentItems.add(e.getKey());
            }
        }
        System.out.println("Frequent 1-itemsets: " + frequentItems.size());

        // Step 4: Hash pairs into buckets
        int numBuckets = 5000;  // hash bucket size
        int[] hashBuckets = new int[numBuckets];

        for (List<String> items : transactions.values()) {
            List<String> filtered = new ArrayList<>();
            for (String item : items)
                if (frequentItems.contains(item))
                    filtered.add(item);

            for (int i = 0; i < filtered.size(); i++) {
                for (int j = i + 1; j < filtered.size(); j++) {
                    int hash = Math.abs((filtered.get(i) + filtered.get(j)).hashCode()) % numBuckets;
                    hashBuckets[hash]++;
                }
            }
        }

        // Step 5: Bitmap of frequent buckets
        boolean[] bitmap = new boolean[numBuckets];
        for (int i = 0; i < numBuckets; i++) {
            if (hashBuckets[i] >= minCount)
                bitmap[i] = true;
        }

        // Step 6: Second Pass — Count candidate pairs that pass bitmap test
        Map<Set<String>, Integer> pairCount = new HashMap<>();

        for (List<String> items : transactions.values()) {
            List<String> filtered = new ArrayList<>();
            for (String item : items)
                if (frequentItems.contains(item))
                    filtered.add(item);

            for (int i = 0; i < filtered.size(); i++) {
                for (int j = i + 1; j < filtered.size(); j++) {
                    int hash = Math.abs((filtered.get(i) + filtered.get(j)).hashCode()) % numBuckets;
                    if (bitmap[hash]) {
                        Set<String> pair = new TreeSet<>();
                        pair.add(filtered.get(i));
                        pair.add(filtered.get(j));
                        pairCount.put(pair, pairCount.getOrDefault(pair, 0) + 1);
                    }
                }
            }
        }

        // Step 7: Collect frequent pairs
        System.out.println("\nFrequent 2-itemsets:");
        for (Map.Entry<Set<String>, Integer> entry : pairCount.entrySet()) {
            if (entry.getValue() >= minCount) {
                double support = (double) entry.getValue() / numTransactions;
                System.out.printf("%s -> Support: %.4f%n", entry.getKey(), support);
            }
        }

        System.out.println("\n✅ PCY Algorithm Completed Successfully!");
    }
}

