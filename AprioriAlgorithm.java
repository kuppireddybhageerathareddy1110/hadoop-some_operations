import java.io.*;
import java.util.*;

/**
 * Apriori Algorithm Implementation for Market Basket Analysis
 * ------------------------------------------------------------
 * Works with semicolon-delimited CSV like:
 * BillNo;Itemname;Quantity;Date;Price;CustomerID;Country
 */
public class AprioriAlgorithm {

    public static void main(String[] args) {
        String filePath = "/home/bhageeratha/Assignment-1_Data.csv";  // absolute path
        double minSupport = 0.02;  // 2% support threshold

        System.out.println("📂 Reading dataset from: " + filePath);

        // Step 1: Load dataset
        Map<String, Set<String>> transactions = loadTransactions(filePath);
        int totalTransactions = transactions.size();
        System.out.println("✅ Total Transactions: " + totalTransactions);

        if (totalTransactions == 0) {
            System.out.println("⚠️ No transactions found. Check CSV delimiter or path.");
            return;
        }

        // Step 2: Generate frequent 1-itemsets
        Map<Set<String>, Integer> freqItemsets = findFrequent1Itemsets(transactions, minSupport, totalTransactions);
        int k = 2;

        // Step 3: Iteratively generate larger itemsets
        while (!freqItemsets.isEmpty()) {
            System.out.println("\n📦 Frequent " + (k - 1) + "-Itemsets:");
            printFrequentItemsets(freqItemsets, totalTransactions);

            Set<Set<String>> candidates = generateCandidates(freqItemsets.keySet(), k);
            if (candidates.isEmpty()) break;

            Map<Set<String>, Integer> candidateCounts = countSupport(candidates, transactions);
            Map<Set<String>, Integer> nextFreqItemsets = new HashMap<>();

            for (Map.Entry<Set<String>, Integer> entry : candidateCounts.entrySet()) {
                double support = (double) entry.getValue() / totalTransactions;
                if (support >= minSupport) {
                    nextFreqItemsets.put(entry.getKey(), entry.getValue());
                }
            }

            if (nextFreqItemsets.isEmpty()) break;
            freqItemsets = nextFreqItemsets;
            k++;
        }

        System.out.println("\n✅ Apriori Algorithm Completed Successfully!");
    }

    // ✅ Corrected for semicolon-delimited CSV
    public static Map<String, Set<String>> loadTransactions(String filePath) {
        Map<String, Set<String>> transactions = new LinkedHashMap<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line = br.readLine(); // Skip header
            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty()) continue;

                // Split by semicolon, not comma
                String[] parts = line.split(";", -1);
                if (parts.length < 2) continue;

                String billNo = parts[0].trim();
                String item = parts[1].trim();

                // Clean text and normalize
                item = item.replaceAll("[^a-zA-Z0-9\\s&\\-\\.]", "").trim();

                transactions.putIfAbsent(billNo, new HashSet<>());
                transactions.get(billNo).add(item);
            }
        } catch (IOException e) {
            System.err.println("❌ Error reading file: " + e.getMessage());
        }

        return transactions;
    }

    // Frequent 1-itemsets
    public static Map<Set<String>, Integer> findFrequent1Itemsets(Map<String, Set<String>> transactions,
                                                                  double minSupport, int totalTransactions) {
        Map<String, Integer> itemCounts = new HashMap<>();

        for (Set<String> items : transactions.values()) {
            for (String item : items) {
                itemCounts.put(item, itemCounts.getOrDefault(item, 0) + 1);
            }
        }

        Map<Set<String>, Integer> frequentItemsets = new HashMap<>();
        for (Map.Entry<String, Integer> entry : itemCounts.entrySet()) {
            double support = (double) entry.getValue() / totalTransactions;
            if (support >= minSupport) {
                Set<String> itemset = new HashSet<>();
                itemset.add(entry.getKey());
                frequentItemsets.put(itemset, entry.getValue());
            }
        }
        return frequentItemsets;
    }

    // Candidate generation
    public static Set<Set<String>> generateCandidates(Set<Set<String>> prevItemsets, int k) {
        Set<Set<String>> candidates = new HashSet<>();
        List<Set<String>> itemsetList = new ArrayList<>(prevItemsets);

        for (int i = 0; i < itemsetList.size(); i++) {
            for (int j = i + 1; j < itemsetList.size(); j++) {
                Set<String> a = itemsetList.get(i);
                Set<String> b = itemsetList.get(j);
                Set<String> union = new HashSet<>(a);
                union.addAll(b);

                if (union.size() == k && allSubsetsFrequent(union, prevItemsets)) {
                    candidates.add(union);
                }
            }
        }
        return candidates;
    }

    private static boolean allSubsetsFrequent(Set<String> itemset, Set<Set<String>> prevItemsets) {
        for (String item : itemset) {
            Set<String> subset = new HashSet<>(itemset);
            subset.remove(item);
            if (!prevItemsets.contains(subset)) return false;
        }
        return true;
    }

    public static Map<Set<String>, Integer> countSupport(Set<Set<String>> candidates,
                                                         Map<String, Set<String>> transactions) {
        Map<Set<String>, Integer> counts = new HashMap<>();

        for (Set<String> candidate : candidates) {
            for (Set<String> transaction : transactions.values()) {
                if (transaction.containsAll(candidate)) {
                    counts.put(candidate, counts.getOrDefault(candidate, 0) + 1);
                }
            }
        }
        return counts;
    }

    public static void printFrequentItemsets(Map<Set<String>, Integer> freqItemsets, int totalTransactions) {
        for (Map.Entry<Set<String>, Integer> entry : freqItemsets.entrySet()) {
            double support = (double) entry.getValue() / totalTransactions;
            System.out.printf("%s -> Support: %.4f%n", entry.getKey(), support);
        }
    }
}

