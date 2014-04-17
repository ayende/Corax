namespace Corax.Queries
{
	public interface ICompareQueryMatches
	{
		void Init(Searcher searcher);
		int Compare(QueryMatch x, QueryMatch y);
	}
}