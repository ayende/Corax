using System;
using System.Collections.Specialized;

namespace Corax.Queries
{
	public class Sorter
	{
		public readonly ICompareQueryMatches[] Comparers;

		public Sorter(string field, bool descending = false)
		{
			Comparers = new ICompareQueryMatches[] { new SortByTerm(field, descending) };
		}

		public Sorter(params ICompareQueryMatches[] comparers)
		{
			Comparers = comparers;
		}
	}

	public interface ICompareQueryMatches
	{
		void Init(Searcher searcher);
		int Compare(QueryMatch x, QueryMatch y);
	}

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

	public class SortByTerm : ICompareQueryMatches
	{
		public readonly string Field;
		public readonly bool Descending;
		private Searcher theSearcer;
		private int fieldNumber;
		private int _factor;

		public SortByTerm(string field, bool descending = false)
		{
			Field = field;
			Descending = descending;
		}

		public void Init(Searcher searcher)
		{
			theSearcer = searcher;
			fieldNumber = searcher.Index.GetFieldNumber(Field);
			_factor = (Descending ? 1 : -1);
		}

		public int Compare(QueryMatch x, QueryMatch y)
		{
			if (theSearcer == null)
				throw new InvalidOperationException("Cannot compare without first getting the searcher we are comparing with");

			var xVal = theSearcer.GetTermForDocument(x.DocumentId, fieldNumber);
			var yVal = theSearcer.GetTermForDocument(y.DocumentId, fieldNumber);

			if (xVal == null && yVal == null)
				return 0;


			if (xVal == null)
				return -1 * _factor;

			if (yVal == null)
				return 1 * _factor;

			var result = xVal.CompareTo(yVal) * _factor;

			return result;
		}
	}
}