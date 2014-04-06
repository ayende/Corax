using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Corax.Utils;
using Voron;
using Voron.Impl;

namespace Corax.Queries
{
	public class Searcher : IDisposable
	{
		private readonly FullTextIndex _index;
		private readonly Transaction _tx;

		public Searcher(FullTextIndex index)
		{
			_index = index;
			_tx = _index.StorageEnvironment.NewTransaction(TransactionFlags.Read);
		}

		public QueryResults QueryTop(Query query, int take, IndexingConventions.ScorerCalc score = null)
		{
			if (take < 0)
				throw new ArgumentException("Take must be non negative");

			var qr = new QueryResults();
			var heap = new Heap<QueryMatch>(take, (x, y) => x.Score.CompareTo(y.Score));
			foreach (var match in Query(query, score))
			{
				heap.Enqueue(match);
				qr.TotalResults++;
			}
			qr.Results = new QueryMatch[heap.Count];
			int pos = 0;
			while (heap.Count > 0)
			{
				qr.Results[pos++] = heap.Dequeue();
			}
			return qr;
		}

		public IEnumerable<QueryMatch> Query(Query query, IndexingConventions.ScorerCalc score = null)
		{
			query.Initialize(_index, _tx, score ?? new DefaultScorer(_index.Conventions).Score);
			return query.Execute();
		}

		private class DefaultScorer
		{
			private readonly IndexingConventions _conventions;
			private readonly Dictionary<int, float> cache = new Dictionary<int, float>();

			public DefaultScorer(IndexingConventions conventions)
			{
				_conventions = conventions;
			}

			public float Score(float queryWeight, int termFreq, float boost)
			{
				float value;
				if (cache.TryGetValue(termFreq, out value))
					return value;
				return cache[termFreq] = queryWeight * _conventions.Tf(termFreq) * boost;
			}
		}

		public void Dispose()
		{
			_tx.Dispose();
		}
	}
}