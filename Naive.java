import java.io.*;
import java.util.*;

public class Naive {
    public static void main(String[] args) {
        String filePath = "Groceries_dataset.csv";
        double minSupport = 0.01; // 1%

        // Step 1: Read transactions grouped by Member_number
        List<Set<String>> transactions = readTransactions(filePath);
        int total = transactions.size();
        System.out.println("✅ Total Transactions: " + total);

        Map<Set<String>, Integer> freqItemsets = new HashMap<>();

        // Step 2: Count single items
        Map<String, Integer> itemCount = new HashMap<>();
        for (Set<String> t : transactions)
            for (String item : t)
                itemCount.put(item, itemCount.getOrDefault(item, 0) + 1);

        // Step 3: Store frequent 1-itemsets
        for (Map.Entry<String, Integer> e : itemCount.entrySet()) {
            Set<String> single = new HashSet<>();
            single.add(e.getKey());
            freqItemsets.put(single, e.getValue());
        }

        // Step 4: Generate and count 2-itemsets
        List<String> items = new ArrayList<>(itemCount.keySet());
        for (int i = 0; i < items.size(); i++) {
            for (int j = i + 1; j < items.size(); j++) {
                String a = items.get(i), b = items.get(j);
                int count = 0;
                for (Set<String> t : transactions)
                    if (t.contains(a) && t.contains(b))
                        count++;
                if ((double) count / total >= minSupport) {
                    Set<String> pair = new HashSet<>(Arrays.asList(a, b));
                    freqItemsets.put(pair, count);
                }
            }
        }

        // Step 5: Display frequent itemsets
        System.out.println("\n✅ Frequent Itemsets:");
        for (Map.Entry<Set<String>, Integer> e : freqItemsets.entrySet()) {
            double support = (double) e.getValue() / total;
            if (support >= minSupport)
                System.out.printf("%s -> Support: %.4f%n", e.getKey(), support);
        }
    }

    static List<Set<String>> readTransactions(String filePath) {
        Map<String, Set<String>> userToItems = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            br.readLine(); // skip header
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

