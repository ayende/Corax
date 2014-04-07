using System;

namespace Corax.Queries
{
	public class QueryMatch : IComparable<QueryMatch>
	{
		public long DocumentId;
		public float Score;

		public int CompareTo(QueryMatch other)
		{
			return Score.CompareTo(other.Score);
		}
	}
}