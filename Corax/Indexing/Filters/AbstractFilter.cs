namespace Corax.Indexing.Filters
{
	public interface IFilter
	{
		bool ProcessTerm(ITokenSource source);
	}
}