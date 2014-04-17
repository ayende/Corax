using System;
using System.Collections.Generic;
using System.Linq;
using Voron;
using Voron.Trees;
using Voron.Util.Conversion;

namespace Corax.Queries
{
	public class InQuery : Query
	{
		private readonly List<string> _values;
		private readonly string _field;
		private Tree _fieldTree;
		private float _weight;
		private readonly byte[] _fieldDocumentBuffer = new byte[FullTextIndex.FieldDocumentSize];

		public InQuery(string field, params string[] values)
		{
			if (field == null) throw new ArgumentNullException("field");
			if (values == null) throw new ArgumentNullException("values");

			_field = field;
			_values = new List<string>(values);
		}

		protected override void Init()
		{
			_fieldTree = Transaction.ReadTree("@fld_" + _field);
			if (_fieldTree == null)
				return;

			var termFreqInDocs = _fieldTree.State.EntriesCount;
			var numberOfDocs = Transaction.ReadTree("$metadata").Read(Transaction, "docs").Reader.ReadInt64();

			var idf = Index.Conventions.Idf(termFreqInDocs, numberOfDocs);
			_weight = idf*idf;

			_values.Sort();
		}

		public override IEnumerable<QueryMatch> Execute()
		{
			if (_values.Count == 0 || _fieldTree == null)
				return Enumerable.Empty<QueryMatch>();

			var result = SingleQueryMatch(_values[0]);

			for (var i = 1; i < _values.Count; i++)
			{
				result = result.Union(SingleQueryMatch(_values[i]), QueryMatchComparer.Instance);
			}

			return result;
		}

		private IEnumerable<QueryMatch> SingleQueryMatch(string value)
		{
			using (var it = _fieldTree.MultiRead(Transaction, value))
			{
				if (it.Seek(Slice.BeforeAllKeys) == false)
					yield break; // yield break;
				do
				{
					it.CurrentKey.CopyTo(_fieldDocumentBuffer);

					var termFreq = EndianBitConverter.Big.ToInt32(_fieldDocumentBuffer, sizeof(long));
					var boost = EndianBitConverter.Big.ToSingle(_fieldDocumentBuffer, sizeof(long) + sizeof(int));

					yield return new QueryMatch
					{
						DocumentId = EndianBitConverter.Big.ToInt64(_fieldDocumentBuffer, 0),
						Score = Score(_weight, termFreq, boost*Boost)
					};
				} while (it.MoveNext());
			}
		}
	}
}