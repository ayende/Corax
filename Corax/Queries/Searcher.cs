using System;
using System.Collections.Generic;
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

			var fieldNumbers = new int[sortBy.Terms.Length];
			for (var i = 0; i < sortBy.Terms.Length; i++)
				fieldNumbers[i] = _index.GetFieldNumber(sortBy.Terms[i].Field);

			return (x, y) =>
			{
				for (int i = 0; i < sortBy.Terms.Length; i++)
				{
					var term = sortBy.Terms[i];
					int fieldNumber = fieldNumbers[i];

					var xVal = GetTermForDocument(x.DocumentId, fieldNumber);
					var yVal = GetTermForDocument(y.DocumentId, fieldNumber);

					if (xVal == null && yVal == null)
						continue;

					var factor = (term.Descending ? 1 : -1);

					if (xVal == null)
						return -1 * factor;

					if (yVal == null)
						return 1 * factor;

					var result = xVal.CompareTo(yVal) * factor;

					if (result != 0)
						return result;
				}

				return 0;
			};
		}

		public ValueReader GetTermForDocument(long docId, int fieldId)
		{
			var buffer = _index.BufferPool.Take(FullTextIndex.DocumentFieldSize);
			try
			{
				EndianBitConverter.Big.CopyBytes(docId, buffer, 0);
				EndianBitConverter.Big.CopyBytes(fieldId, buffer, sizeof(long));
				EndianBitConverter.Big.CopyBytes(0, buffer, sizeof(long) + sizeof(int));

				using (var it = _docs.Iterate(_tx))
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

	public class Sorter
	{
		public readonly SortTerm[] Terms;

		public Sorter(string field, bool descending = false)
		{
			Terms = new[] { new SortTerm(field, descending) };
		}

		public Sorter(params SortTerm[] terms)
		{
			Terms = terms;
		}
	}

	public class SortTerm
	{
		public readonly string Field;
		public readonly bool Descending;

		public SortTerm(string field, bool descending = false)
		{
			Field = field;
			Descending = descending;
		}
	}
}