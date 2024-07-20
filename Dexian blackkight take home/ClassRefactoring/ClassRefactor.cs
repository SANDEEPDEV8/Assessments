using System;

namespace DeveloperSample.ClassRefactoring;

public enum SwallowType
{
    African, European
}

public enum SwallowLoad
{
    None, Coconut
}

public interface ISwallow
{
    SwallowType Type { get; }
    SwallowLoad Load { get; }
    double GetAirspeedVelocity();
    void ApplyLoad(SwallowLoad load);
}

public class AfricanSwallow : ISwallow
{
    public SwallowType Type => SwallowType.African;
    public SwallowLoad Load { get; private set; }

    public double GetAirspeedVelocity()
    {
        if (Load == SwallowLoad.None)
        {
            return 22;
        }
        if (Load == SwallowLoad.Coconut)
        {
            return 18;
        }
        throw new InvalidOperationException();
    }

    public void ApplyLoad(SwallowLoad load)
    {
        Load = load;
    }
}

public class EuropeanSwallow : ISwallow
{
    public SwallowType Type => SwallowType.European;
    public SwallowLoad Load { get; private set; }

    public double GetAirspeedVelocity()
    {
        if (Load == SwallowLoad.None)
        {
            return 20;
        }
        if (Load == SwallowLoad.Coconut)
        {
            return 16;
        }
        throw new InvalidOperationException();
    }

    public void ApplyLoad(SwallowLoad load)
    {
        Load = load;
    }
}

public class SwallowFactory
{
    public ISwallow GetSwallow(SwallowType swallowType)
    {
        return swallowType switch
        {
            SwallowType.African => new AfricanSwallow(),
            SwallowType.European => new EuropeanSwallow(),
            _ => throw new InvalidOperationException(),
        };
    }
}