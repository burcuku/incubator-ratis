package org.apache.ratis.inst.failure;

import java.util.Random;

/**
 * From https://github.com/fniksic/zk-tester
 */
public abstract class FailureGenerator {

    private final int[][] totalArrangements;
    private final int totalRounds;
    private final int maxFaultsPerRound;
    private final int faultBudget;

    private int remainingRounds;
    private int remainingFaultBudget;

    private final Random random;

    public FailureGenerator(final int totalRounds, final int maxFaultsPerRound, final int faultBudget) {
        this(totalRounds, maxFaultsPerRound, faultBudget, new Random());
    }

    public FailureGenerator(final int totalRounds, final int maxFaultsPerRound, final int faultBudget, final Random random) {
        this.totalArrangements = new int[totalRounds + 1][faultBudget + 1];
        this.maxFaultsPerRound = maxFaultsPerRound;
        this.totalRounds = totalRounds;
        this.faultBudget = faultBudget;
        this.random = random;
        reset();
        computeArrangements();
    }

    public void reset() {
        remainingRounds = totalRounds;
        remainingFaultBudget = faultBudget;
    }

    public int generate() {
        if (remainingRounds == 0) {
            return 0;
        }

        if (totalArrangements[remainingRounds][remainingFaultBudget] == 0) {
            // There are too many remaining faults, they cannot be arranged into the remaining
            // rounds given maxFaultsPerRound
            assert remainingFaultBudget > maxFaultsPerRound;
            remainingFaultBudget -= maxFaultsPerRound;
            --remainingRounds;
            return maxFaultsPerRound;
        }

        final int maxFaults = Math.min(maxFaultsPerRound, remainingFaultBudget);
        final double[] cumulativeProbabilities = new double[maxFaults + 1];
        int sum = 0;
        final double total = Integer.valueOf(totalArrangements[remainingRounds][remainingFaultBudget]).doubleValue();
        for (int i = 0; i <= maxFaults; ++i) {
            sum += totalArrangements[remainingRounds - 1][remainingFaultBudget - i];
            cumulativeProbabilities[i] = Integer.valueOf(sum).doubleValue() / total;
        }

        final double p = random.nextDouble();
        final int faults = inverseCumulativeProbability(cumulativeProbabilities, p);
        remainingFaultBudget -= faults;
        --remainingRounds;
        return faults;
    }

    /***
     * Populates the table totalArrangements so that at the end the number
     * totalArrangements[n][d] is equal to the number of ways of distributing d balls (faults) into
     * n bins (rounds), with the restriction that each bin can contain at most maxFaultsPerRound balls.
     */
    private void computeArrangements() {
        initalizeArrangements(totalArrangements[0]);
        for (int i = 1; i <= remainingRounds; ++i) {
            int sum = 0;
            for (int j = 0; j <= remainingFaultBudget; ++j) {
                sum += totalArrangements[i - 1][j];
                if (j - maxFaultsPerRound > 0) {
                    sum -= totalArrangements[i - 1][j - maxFaultsPerRound - 1];
                }
                totalArrangements[i][j] = sum;
            }
        }
    }

    protected abstract void initalizeArrangements(final int[] arrangementsZerothRow);

    /***
     * Finds the least index <code>i</code> such that <code>p < cumulativeProbabilities[i]</code>.
     *
     * @param cumulativeProbabilities
     * @param p
     * @return
     */
    private int inverseCumulativeProbability(final double[] cumulativeProbabilities, double p) {
        int low = 0;
        int high = cumulativeProbabilities.length;
        while (low < high) {
            final int mid = (low + high) / 2;
            if (p >= cumulativeProbabilities[mid]) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    public void printArrangement() {
        for(int i = 0; i <= totalRounds; i++) {
            for(int j = 0; j <= faultBudget; j++) {
                System.out.print(totalArrangements[i][j] + " \t\t\t");
            }
            System.out.println("\n");
        }
    }

    public int getRemainingFaultBudget() {
        return remainingFaultBudget;
    }
}
