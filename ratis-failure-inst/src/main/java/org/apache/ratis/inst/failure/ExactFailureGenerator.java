package org.apache.ratis.inst.failure;

import java.util.Random;

public class ExactFailureGenerator extends FailureGenerator {

    public ExactFailureGenerator(final int totalRounds, final int maxFaultsPerRound, final int faultBudget, final Random random) {
        super(totalRounds, maxFaultsPerRound, faultBudget, random);
    }

    @Override
    protected void initalizeArrangements(final int[] arrangementsZerothRow) {
        arrangementsZerothRow[0] = 1;
    }
}
