# apriori_spark.py
from pyspark.sql import SparkSession
from itertools import combinations
from typing import List, Set, Tuple

def apriori_spark(
    transactions_rdd,
    min_support: float = 0.01,
    min_confidence: float = 0.6
):
    total_transactions = transactions_rdd.count()
    min_count = int(min_support * total_transactions)

    # ---- Frequent 1-itemsets ----
    freq1 = transactions_rdd.flatMap(lambda t: [(item, 1) for item in t]) \
                            .reduceByKey(lambda a, b: a + b) \
                            .filter(lambda x: x[1] >= min_count)

    frequent_items = freq1.map(lambda x: frozenset([x[0]])).collect()
    frequent_itemsets = set(frequent_items)
    support_dict = dict(freq1.map(lambda x: (frozenset([x[0]]), x[1] / total_transactions)).collect())

    k = 2
    Lk = frequent_items

    while len(Lk) > 0:
        candidates = generate_candidates(Lk, k)
        candidate_counts = transactions_rdd.flatMap(
            lambda t: [(cand, 1) for cand in candidates if cand.issubset(t)]
        ).reduceByKey(lambda a, b: a + b) \
         .filter(lambda x: x[1] >= min_count)

        current_lk = candidate_counts.map(lambda x: x[0]).collect()
        for itemset, count in candidate_counts.collect():
            support_dict[itemset] = count / total_transactions
            frequent_itemsets.add(itemset)

        Lk = current_lk
        k += 1

    rules = generate_rules(frequent_itemsets, support_dict, min_confidence)
    return frequent_itemsets, support_dict, rules


def generate_candidates(Lk_minus_1: List[frozenset], k: int):
    candidates = set()
    for i in range(len(Lk_minus_1)):
        for j in range(i + 1, len(Lk_minus_1)):
            s1 = sorted(Lk_minus_1[i])
            s2 = sorted(Lk_minus_1[j])
            if s1[:k-2] == s2[:k-2]:
                cand = frozenset(s1 + [s2[-1]])
                if all(frozenset(c) in Lk_minus_1 for c in combinations(cand, k-1)):
                    candidates.add(cand)
    return list(candidates)


def generate_rules(frequent_itemsets, support_dict, min_confidence):
    rules = []
    for itemset in frequent_itemsets:
        if len(itemset) > 1:
            for conseq_size in range(1, len(itemset)):
                for conseq in combinations(itemset, conseq_size):
                    conseq = frozenset(conseq)
                    ante = itemset - conseq
                    if ante in support_dict and support_dict[ante] > 0:
                        conf = support_dict[itemset] / support_dict[ante]
                        if conf >= min_confidence:
                            lift = conf / support_dict[conseq] if support_dict[conseq] > 0 else 0
                            rules.append((ante, conseq, support_dict[itemset], conf, lift))
    return rules


# ================================
# MAIN
# ================================
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Hadoop_Apriori") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    # === READ FROM HDFS ===
    data_path = "hdfs:///user/bhageeratha/transactions.csv"
    df = spark.read.csv(data_path, inferSchema=False)
    transactions_rdd = df.rdd.map(lambda row: set(row[0].split(',')))

    print(f"Loaded {transactions_rdd.count()} transactions from HDFS")

    freqs, supports, rules = apriori_spark(
        transactions_rdd,
        min_support=0.3,
        min_confidence=0.7
    )

    print("\n=== Frequent Itemsets ===")
    for itemset in sorted(freqs, key=lambda x: supports[x], reverse=True):
        print(f"{set(itemset)} => {supports[itemset]:.2%}")

    print("\n=== Association Rules ===")
    for ant, cons, sup, conf, lift in rules:
        print(f"{set(ant)} => {set(cons)} | sup: {sup:.2%}, conf: {conf:.2%}, lift: {lift:.2f}")

    spark.stop()
