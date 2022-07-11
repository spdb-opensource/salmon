package com.spdb.hive.sync.util.hive;

import java.io.Serializable;
import java.util.*;

/**
 * project：hive-test
 * package：com.spdb.hive.replication.util.hive
 * author：zhouxh
 * time：2021-03-31 11:14
 * description：
 */
public class TBaseHelperUtil {

    private static final Comparator comparator = new NestedStructureComparator();

    /**
     * 重载TBaseHelper中的compareTo(Map a, Map b)
     * 这里主要针对table和partition的parameter熟悉的比较做出调整
     * 去除掉parameter中的一些统计信息
     *
     * @param a 源map
     * @param b 目标map
     * @return 返回比较结果
     */
    public static int compareTo(Map a, Map b) {

        //  将parameter中的统计信息去除
        a = removeStatisticsInfo(a);
        b = removeStatisticsInfo(b);

        int lastComparison = compareTo(a.size(), b.size());
        if (lastComparison != 0) {
            return lastComparison;
        }

        // Sort a and b so we can compare them.
        SortedMap sortedA = new TreeMap(comparator);
        sortedA.putAll(a);
        Iterator<Map.Entry> iterA = sortedA.entrySet().iterator();
        SortedMap sortedB = new TreeMap(comparator);
        sortedB.putAll(b);
        Iterator<Map.Entry> iterB = sortedB.entrySet().iterator();

        // Compare each item.
        while (iterA.hasNext() && iterB.hasNext()) {
            Map.Entry entryA = iterA.next();
            Map.Entry entryB = iterB.next();
            lastComparison = comparator.compare(entryA.getKey(), entryB.getKey());
            if (lastComparison != 0) {
                return lastComparison;
            }
            lastComparison = comparator.compare(entryA.getValue(), entryB.getValue());
            if (lastComparison != 0) {
                return lastComparison;
            }
        }

        return 0;
    }

    public static int compareTo(int a, int b) {
        if (a < b) {
            return -1;
        } else if (b < a) {
            return 1;
        } else {
            return 0;
        }
    }

    public static int compareTo(List a, List b) {
        int lastComparison = compareTo(a.size(), b.size());
        if (lastComparison != 0) {
            return lastComparison;
        }
        for (int i = 0; i < a.size(); i++) {
            lastComparison = comparator.compare(a.get(i), b.get(i));
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        return 0;
    }

    public static int compareTo(Set a, Set b) {
        int lastComparison = compareTo(a.size(), b.size());
        if (lastComparison != 0) {
            return lastComparison;
        }
        SortedSet sortedA = new TreeSet(comparator);
        sortedA.addAll(a);
        SortedSet sortedB = new TreeSet(comparator);
        sortedB.addAll(b);

        Iterator iterA = sortedA.iterator();
        Iterator iterB = sortedB.iterator();

        // Compare each item.
        while (iterA.hasNext() && iterB.hasNext()) {
            lastComparison = comparator.compare(iterA.next(), iterB.next());
            if (lastComparison != 0) {
                return lastComparison;
            }
        }

        return 0;
    }

    public static int compareTo(byte[] a, byte[] b) {
        int sizeCompare = compareTo(a.length, b.length);
        if (sizeCompare != 0) {
            return sizeCompare;
        }
        for (int i = 0; i < a.length; i++) {
            int byteCompare = compareTo(a[i], b[i]);
            if (byteCompare != 0) {
                return byteCompare;
            }
        }
        return 0;
    }

    public static int compareTo(Comparable a, Comparable b) {
        return a.compareTo(b);
    }

    /**
     * 移除parameter中的统计信息
     *
     * @param parameter
     * @return
     */
    public static Map removeStatisticsInfo(Map parameter) {

        parameter.remove("totalSize");
        parameter.remove("numRows");
        parameter.remove("rawDataSize");
        parameter.remove("numFiles");
        parameter.remove("COLUMN_STATS_ACCURATE");
        parameter.remove("numFilesErasureCoded");
        parameter.remove("bucketing_version");
        parameter.remove("STATS_GENERATED_VIA_STATS_TASK");

        return parameter;
    }

    private static class NestedStructureComparator implements Comparator, Serializable {
        public int compare(Object oA, Object oB) {
            if (oA == null && oB == null) {
                return 0;
            } else if (oA == null) {
                return -1;
            } else if (oB == null) {
                return 1;
            } else if (oA instanceof List) {
                return compareTo((List) oA, (List) oB);
            } else if (oA instanceof Set) {
                return compareTo((Set) oA, (Set) oB);
            } else if (oA instanceof Map) {
                return compareTo((Map) oA, (Map) oB);
            } else if (oA instanceof byte[]) {
                return compareTo((byte[]) oA, (byte[]) oB);
            } else {
                return compareTo((Comparable) oA, (Comparable) oB);
            }
        }
    }

    public static void main(String[] args) {
        ArrayList<String> a=new ArrayList<String>();
        ArrayList<String> b=new ArrayList<String>();

        a.add("aaa");
        a.add("bbb");

        b.add("aaa");
        b.add("bbb");

        int i = compareTo(a, b);

        System.out.println(i);

    }
}
