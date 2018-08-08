package ideal.sylph.spi;

public interface RunnerFactory
{
    Runner create(RunnerContext context);
}
