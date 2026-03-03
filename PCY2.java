import java.io.*;
import java.util.*;

public class PCY2 {
    public static void main(String[] args) {
        String filePath = "Groceries_dataset.csv";
        double minSupport = 0.01;
        int hashBuckets = 10000;

        List<Set<String>> transactions = readTransactions(filePath);
        int total = transactions.size();
        System.out.println("✅ Total Transactions: " + total);

        // Pass 1: Count single items and hash pairs
        Map<String, Integer> itemCount = new HashMap<>();
        int[] hashTable = new int[hashBuckets];

        for (Set<String> t : transactions) {
            List<String> items = new ArrayList<>(t);
            for (String item : items)
                itemCount.put(item, itemCount.getOrDefault(item, 0) + 1);

            for (int i = 0; i < items.size(); i++) {
                for (int j = i + 1; j < items.size(); j++) {
                    int hash = Math.abs((items.get(i) + items.get(j)).hashCode()) % hashBuckets;
                    hashTable[hash]++;
                }
            }
        }

        // Identify frequent buckets
        Set<Integer> frequentBuckets = new HashSet<>();
        for (int i = 0; i < hashBuckets; i++)
            if ((double) hashTable[i] / total >= minSupport)
                frequentBuckets.add(i);

        // Pass 2: Generate candidate pairs
        List<String> frequentItems = new ArrayList<>();
        for (Map.Entry<String, Integer> e : itemCount.entrySet())
            if ((double) e.getValue() / total >= minSupport)
                frequentItems.add(e.getKey());

        Map<Set<String>, Integer> pairCount = new HashMap<>();
        for (Set<String> t : transactions) {
            List<String> items = new ArrayList<>(t);
            for (int i = 0; i < items.size(); i++) {
                for (int j = i + 1; j < items.size(); j++) {
                    int hash = Math.abs((items.get(i) + items.get(j)).hashCode()) % hashBuckets;
                    if (frequentBuckets.contains(hash)) {
                        Set<String> pair = new HashSet<>(Arrays.asList(items.get(i), items.get(j)));
                        pairCount.put(pair, pairCount.getOrDefault(pair, 0) + 1);
                    }
                }
            }
        }

        System.out.println("\n✅ Frequent Pairs (PCY):");
        for (Map.Entry<Set<String>, Integer> e : pairCount.entrySet()) {
            double support = (double) e.getValue() / total;
            if (support >= minSupport)
                System.out.printf("%s -> Support: %.4f%n", e.getKey(), support);
        }

        System.out.println("\n✅ PCY Algorithm Completed Successfully!");
    }

    static List<Set<String>> readTransactions(String filePath) {
        Map<String, Set<String>> userToItems = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            br.readLine();
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length < 3) continue;
                String member = parts[0].trim();
                String item = parts[2].trim();
                if (!item.isEmpty()) {
                    userToItems.computeIfAbsent(member, k -> new HashSet<>()).add(item);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ArrayList<>(userToItems.values());
    }
}
