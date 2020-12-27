package limits;

import model.Limits;

import java.time.Duration;

public class LimitsHolder {

    private int suppliersCount = 0;

    private final Limits limits = new Limits(
        Duration.ofSeconds(90).toMillis(),
        10
    );

    public Limits increment() {
        suppliersCount++;

        if (suppliersCount == 0) {
            return limits;
        }

        return new Limits(
            limits.timeRangeMillis,
            (long) Math.floor(limits.executionCount / suppliersCount)
        );
    }

    public Limits decrement() {
        suppliersCount--;

        if (suppliersCount == 0) {
            return limits;
        }

        return new Limits(
            limits.timeRangeMillis,
            (long) Math.floor(limits.executionCount / suppliersCount)
        );
    }

}
