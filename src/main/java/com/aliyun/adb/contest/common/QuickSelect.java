package com.aliyun.adb.contest.common;

public class QuickSelect {

    // Standard Lomuto partition function
    static int partition(long[] array, int low, int high) {
        long temp;
        long pivot = array[high];
        int i = (low - 1);
        for (int j = low; j <= high - 1; j++) {
            if (array[j] <= pivot) {
                i++;
                temp = array[i];
                array[i] = array[j];
                array[j] = temp;
            }
        }

        temp = array[i + 1];
        array[i + 1] = array[high];
        array[high] = temp;

        return (i + 1);
    }

    // Implementation of QuickSelect
    public static long quick_select(long[] array, int left, int right, int k) {
        while (left <= right) {

            // Partition a[left..right] around a pivot
            // and find the position of the pivot
            int pivotIndex = partition(array, left, right);

            // If pivot itself is the k-th smallest element
            if (pivotIndex == k)
                return array[pivotIndex];

                // If there are more than k-1 elements on
                // left of pivot, then k-th smallest must be
                // on left side.
            else if (pivotIndex > k)
                right = pivotIndex - 1;

                // Else k-th smallest is on right side.
            else
                left = pivotIndex + 1;
        }
        return -1;
    }

}
