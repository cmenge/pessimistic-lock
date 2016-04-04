using System;

namespace Shared
{
    /// <summary>
    /// These are dead-simple CodeContract stubs that can be used if CodeContracts aren't available, especially with Mono,
    /// while still leaving the actual check in place. That isn't nearly as elegant as the actual code contracts, but since
    /// all too fancy rewriting and static checks usually only work for smallish libraries anyway, this shouldn't be much
    /// of a concern.
    /// </summary>
    public static class Checker
    {
        public static void Requires<TException>(bool check) where TException : Exception, new()
        {
            if (!check)
                throw new TException();
        }

        public static void Requires<TException>(bool check, string message) where TException : Exception, new()
        {
            if (!check)
                throw ((TException)Activator.CreateInstance(typeof(TException), message));
            //throw new TException();
        }

        public static void Assume(bool foo)
        {
        }

        public static void Assert(bool check)
        {
            if (!check)
                throw new Exception("Assert Failed!");
        }

        public static void Ensures(bool foo)
        {
        }

        public static T Result<T>()
        {
            return default(T);
        }
    }
}
