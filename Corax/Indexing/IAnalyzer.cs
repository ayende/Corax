namespace Corax.Indexing
{
	public interface IAnalyzer
	{
		ITokenSource CreateTokenSource(string field, ITokenSource existing);
		bool Process(string field, ITokenSource source);

	}
}