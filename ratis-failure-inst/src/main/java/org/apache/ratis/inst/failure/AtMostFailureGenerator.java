package org.apache.ratis.inst.failure;

import java.util.Arrays;
import java.util.Random;

public class AtMostFailureGenerator extends FailureGenerator {

    public AtMostFailureGenerator(final int totalRounds, final int maxFaultsPerRound, final int faultBudget,
                                final Random random) {
        super(totalRounds, maxFaultsPerRound, faultBudget, random);
    }

    @Override
    protected void initalizeArrangements(final int[] arrangementsZerothRow) {
        Arrays.fill(arrangementsZerothRow, 1);
    }
}
