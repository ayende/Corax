namespace Corax.Queries
{
	public class SortByScore : ICompareQueryMatches
	{
		public void Init(Searcher searcher)
		{

		}
 
		public int Compare(QueryMatch x, QueryMatch y)
		{
			return x.Score.CompareTo(y.Score);
		}
	}
}