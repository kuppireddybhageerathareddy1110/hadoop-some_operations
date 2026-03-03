import java.io.*;
import java.util.*;

/**
 * Naive Frequent Itemset Mining (Brute Force)
 * -------------------------------------------
 * Reads transactions from 'Assignment-1_Data.csv' (semicolon-separated)
 * and finds frequent 1-itemsets and 2-itemsets using brute-force counting.
 *
 * Columns: BillNo;Itemname;Quantity;Date;Price;CustomerID;Country
 */
public class NaiveAlgorithm {

    public static void main(String[] args) {
        String filePath = "Assignment-1_Data.csv";  // Input CSV
        double minSupport = 0.01;                   // 1% support threshold

        // Step 1: Load transactions
        Map<String, Set<String>> transactions = loadTransactions(filePath);
        int totalTransactions = transactions.size();
        System.out.println("✅ Total Transactions: " + totalTransactions);

        if (totalTransactions == 0) {
            System.out.println("⚠️ No transactions found. Check CSV format or path.");
            return;
        }

        // Step 2: Find frequent 1-itemsets
        Map<String, Integer> itemCounts = new HashMap<>();
        for (Set<String> items : transactions.values()) {
            for (String item : items) {
                itemCounts.put(item, itemCounts.getOrDefault(item, 0) + 1);
            }
        }

        Map<Set<String>, Integer> freq1 = new LinkedHashMap<>();
        for (Map.Entry<String, Integer> entry : itemCounts.entrySet()) {
            double support = (double) entry.getValue() / totalTransactions;
            if (support >= minSupport) {
                // ✅ Java 8 compatible way instead of Set.of()
                Set<String> singleItem = new HashSet<>();
                singleItem.add(entry.getKey());
                freq1.put(singleItem, entry.getValue());
            }
        }

        System.out.println("\n📦 Frequent 1-Itemsets:");
        printFrequent(freq1, totalTransactions);

        // Step 3: Find frequent 2-itemsets (brute force)
        Map<Set<String>, Integer> pairCounts = new HashMap<>();
        List<String> items = new ArrayList<>(itemCounts.keySet());

        for (int i = 0; i < items.size(); i++) {
            for (int j = i + 1; j < items.size(); j++) {
                String itemA = items.get(i);
                String itemB = items.get(j);
                Set<String> pair = new HashSet<>(Arrays.asList(itemA, itemB));

                int count = 0;
                for (Set<String> transaction : transactions.values()) {
                    if (transaction.contains(itemA) && transaction.contains(itemB)) {
                        count++;
                    }
                }

                double support = (double) count / totalTransactions;
                if (support >= minSupport) {
                    pairCounts.put(pair, count);
                }
            }
        }

        System.out.println("\n📦 Frequent 2-Itemsets:");
        printFrequent(pairCounts, totalTransactions);

        System.out.println("\n✅ Naive Algorithm Completed Successfully!");
    }

    // Reads CSV and groups items by BillNo
    public static Map<String, Set<String>> loadTransactions(String filePath) {
        Map<String, Set<String>> transactions = new LinkedHashMap<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            br.readLine(); // skip header

            while ((line = br.readLine()) != null) {
                String[] parts = line.split(";", -1);
                if (parts.length < 7) continue;

                String billNo = parts[0].trim();
                String item = parts[1].trim();

                transactions.putIfAbsent(billNo, new HashSet<String>());
                transactions.get(billNo).add(item);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return transactions;
    }

    // Print itemsets with support
    public static void printFrequent(Map<Set<String>, Integer> freq, int total) {
        for (Map.Entry<Set<String>, Integer> entry : freq.entrySet()) {
            double support = (double) entry.getValue() / total;
            System.out.printf("%s -> Support: %.4f%n", entry.getKey(), support);
        }
    }
}

