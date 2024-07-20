using System;
using System.Linq;

namespace DeveloperSample.Algorithms
{
    public static class Algorithms
    {
        public static int GetFactorial(int n)
        {
            if (n < 0)
            {
                throw new ArgumentException("Invalid input, number cannot be negative");
            }

            int result = 1;

            for (int i = 1; i <= n; i++)
            {
                result *= i;
            }

            return result;
        }

        public static string FormatSeparators(params string[] items)
        {

            if (items == null)
            {
                throw new ArgumentNullException(nameof(items));
            }

            string formatted = string.Join(", ", items.Take(items.Length - 1));

            if (items.Length > 1)
            {
                formatted += " and ";
            }

            formatted += items.LastOrDefault();

            return formatted;
        }
    }
}
