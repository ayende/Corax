using System;
using System.Collections.Generic;
using System.Linq;
using Corax.Utils;
using Voron;
using Voron.Impl;
using Voron.Trees;
using Voron.Util.Conversion;

namespace Corax.Queries
{
	public class Searcher : IDisposable
	{
		private readonly FullTextIndex _index;
		private readonly Transaction _tx;
		private Tree _docs;

		public Searcher(FullTextIndex index)
		{
			_index = index;
			_tx = _index.StorageEnvironment.NewTransaction(TransactionFlags.Read);

			_docs = _tx.ReadTree("Docs");
		}

		public FullTextIndex Index { get { return _index; }}

		public QueryResults QueryTop(Query query, int take,
			IndexingConventions.ScorerCalc score = null,
			Sorter sortBy = null)
		{
			if (take < 0)
				throw new ArgumentException("Take must be non negative");

			var qr = new QueryResults();
			var heap = new Heap<QueryMatch>(take, GenerateComparisonFunction(sortBy));
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

		private Comparison<QueryMatch> GenerateComparisonFunction(Sorter sortBy)
		{
			if (sortBy == null)
				return (x, y) => x.Score.CompareTo(y.Score);

			foreach (var sorter in sortBy.Comparers)
			{
				sorter.Init(this);
			}

			return (x, y) => sortBy.Comparers.Select(term => term.Compare(x, y))
				.FirstOrDefault(ret => ret != 0);
		}

		public ValueReader? GetTermForDocument(long docId, int fieldId)
		{
			var buffer = _index.BufferPool.Take(FullTextIndex.DocumentFieldSize);
			try
			{
				EndianBitConverter.Big.CopyBytes(docId, buffer, 0);
				EndianBitConverter.Big.CopyBytes(fieldId, buffer, sizeof(long));
				EndianBitConverter.Big.CopyBytes(0, buffer, sizeof(long) + sizeof(int));

				using (var it = _docs.Iterate())
				{
					if (it.Seek(new Slice(buffer)) == false)
						return null;

					it.CurrentKey.CopyTo(buffer);

					if (EndianBitConverter.Big.ToInt64(buffer, 0) != docId)
						return null;

					return it.CreateReaderForCurrent();
				}
			}
			finally
			{
				_index.BufferPool.Return(buffer);
			}
		}

		public IEnumerable<QueryMatch> Query(Query query, IndexingConventions.ScorerCalc score = null, Sorter sortby = null)
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