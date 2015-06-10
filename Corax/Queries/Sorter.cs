using System.Collections.Specialized;

namespace Corax.Queries
{
    public class Sorter
    {
        public readonly ICompareQueryMatches[] Comparers;

        public Sorter(string field, bool descending = false)
        {
            //Comparers = new ICompareQueryMatches[] { new SortByTerm(field, descending) };
        }

        public Sorter(params ICompareQueryMatches[] comparers)
        {
            Comparers = comparers;
        }
    }
}