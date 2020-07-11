package ideal.sylph.etl.api;

/**
 * condition sink,you can extend this by your owm condition ,like time oe num ..
 * created by Jfan chao
 *
 */
public abstract class ConditionRealTimeSink implements RealTimeSink{
    /**
     * Condition for sink invoke.
     *
     * @return value of the updating condition
     */
    protected abstract boolean updateCondition();

    /**
     * Statements to be executed after writing a batch goes here.
     */
    protected abstract void resetParameters();
}
