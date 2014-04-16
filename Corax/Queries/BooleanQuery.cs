using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Corax.Queries
{
	public class BooleanQuery : Query
	{
		public BooleanQuery(QueryOperator op, Query left, Query right)
		{
			Op = op;
			Left = left;
			Right = right;
		}

		public QueryOperator Op { get; set; }
		public Query Left { get; set; }
		public Query Right { get; set; }

		protected override void Init()
		{
			Left.Initialize(Index, Transaction, Score);
			Right.Initialize(Index, Transaction, Score);
		}

		public override IEnumerable<QueryMatch> Execute()
		{
			var leftResults = Left.Execute();
			var rightResults = Right.Execute();
			if (Op == QueryOperator.And)
			{
				return leftResults.Intersect(rightResults, new QueryMatchComparer());
			} else if (Op == QueryOperator.Or)
			{
				return leftResults.Union(rightResults, new QueryMatchComparer());
			}
			else
			{
				throw new InvalidOperationException("QueryOperator must be And or Or.");
			}
		}

		private class QueryMatchComparer : IEqualityComparer<QueryMatch>
		{
			public bool Equals(QueryMatch x, QueryMatch y)
			{
				return x.DocumentId == y.DocumentId;
			}

			public int GetHashCode(QueryMatch match)
			{
				return 0; // We're not using the hash, so return a dummy value.
			}
		}

	}

	public enum QueryOperator
	{
		And, 
		Or
	}
}
