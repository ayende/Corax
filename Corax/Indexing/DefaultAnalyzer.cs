using Corax.Indexing.Filters;

namespace Corax.Indexing
{
	public class DefaultAnalyzer : IAnalyzer
	{
		readonly IFilter[] _filters =
		{
			new LowerCaseFilter(), 
			new RemovePossesiveSuffix(), 
			new StopWordsFilter(), 
		};


		public ITokenSource CreateTokenSource(string field, ITokenSource existing)
		{
			return existing ?? new StringTokenizer();
		}

		public bool Process(string field, ITokenSource source)
		{
			for (int i = 0; i < _filters.Length; i++)
			{
				if (_filters[i].ProcessTerm(source) == false)
					return false;
			}
			return true;
		}
	}
}