import java.io.*;
import java.util.*;

public class AprioriAlgorithm1 {
    public static void main(String[] args) {
        String filePath = "groceries.csv";
        double minSupport = 0.01;

        List<Set<String>> transactions = readTransactions(filePath);
        int total = transactions.size();
        System.out.println("✅ Total Transactions: " + total);

        List<Set<Set<String>>> allFrequent = new ArrayList<>();

        // Step 1: Frequent 1-itemsets
        Map<Set<String>, Integer> L1 = getFrequent1Itemsets(transactions, total, minSupport);
        allFrequent.add(L1.keySet());

        Map<Set<String>, Integer> Lk = L1;
        int k = 2;

        // Step 2: Generate higher-order itemsets
        while (!Lk.isEmpty()) {
            Set<Set<String>> candidates = aprioriGen(Lk.keySet());
            Map<Set<String>, Integer> Ck = new HashMap<>();
            for (Set<String> transaction : transactions)
                for (Set<String> candidate : candidates)
                    if (transaction.containsAll(candidate))
                        Ck.put(candidate, Ck.getOrDefault(candidate, 0) + 1);

            Lk = new HashMap<>();
            for (Map.Entry<Set<String>, Integer> e : Ck.entrySet()) {
                double support = (double) e.getValue() / total;
                if (support >= minSupport)
                    Lk.put(e.getKey(), e.getValue());
            }

            if (!Lk.isEmpty()) allFrequent.add(Lk.keySet());
            k++;
        }

        System.out.println("\n✅ Frequent Itemsets:");
        for (Set<Set<String>> level : allFrequent)
            for (Set<String> itemset : level)
                System.out.println(itemset);
    }

    static Map<Set<String>, Integer> getFrequent1Itemsets(List<Set<String>> transactions, int total, double minSupport) {
        Map<String, Integer> count = new HashMap<>();
        for (Set<String> t : transactions)
            for (String item : t)
                count.put(item, count.getOrDefault(item, 0) + 1);

        Map<Set<String>, Integer> L1 = new HashMap<>();
        for (Map.Entry<String, Integer> e : count.entrySet()) {
            if ((double) e.getValue() / total >= minSupport)
                L1.put(new HashSet<>(Collections.singletonList(e.getKey())), e.getValue());
        }
        return L1;
    }

    static Set<Set<String>> aprioriGen(Set<Set<String>> Lk) {
        Set<Set<String>> candidates = new HashSet<>();
        List<Set<String>> list = new ArrayList<>(Lk);
        for (int i = 0; i < list.size(); i++) {
            for (int j = i + 1; j < list.size(); j++) {
                Set<String> s1 = new HashSet<>(list.get(i));
                Set<String> s2 = new HashSet<>(list.get(j));
                s1.addAll(s2);
                if (s1.size() == list.get(0).size() + 1)
                    candidates.add(s1);
            }
        }
        return candidates;
    }

    static List<Set<String>> readTransactions(String filePath) {
        List<Set<String>> transactions = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            br.readLine(); // skip header
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                Set<String> items = new HashSet<>();
                for (int i = 1; i < parts.length; i++) {
                    String item = parts[i].trim();
                    if (!item.isEmpty()) items.add(item);
                }
                if (!items.isEmpty()) transactions.add(items);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return transactions;
    }
}
