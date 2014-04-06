using System.Collections.Generic;
using Voron;
using Voron.Impl;
using Voron.Trees;
using Voron.Util.Conversion;

namespace Corax.Queries
{
	public class TermQuery : Query
	{
		private readonly string _field;
		private readonly string _value;
		private Tree _fieldTree;
		private float _weight;

		public TermQuery(string field, string value)
		{
			_field = field;
			_value = value;
		}

		public override string ToString()
		{
			return string.Format("{0}: {1}", _field, _value);
		}

		protected override void Init()
		{
			_fieldTree = Transaction.ReadTree("@" + _field);
			if (_fieldTree == null)
				return;
			var termFreqInDocs = _fieldTree.State.EntriesCount;
			var numberOfDocs = Transaction.ReadTree("Documents").State.EntriesCount;

			var idf = Index.Conventions.Idf(termFreqInDocs, numberOfDocs);
			_weight = idf*idf; // square it
		}

		public override IEnumerable<QueryMatch> Execute()
		{
			if (_fieldTree == null)
				yield break;

			var buffer = new byte[sizeof (long) + sizeof (int) + sizeof (float)];
			using (var it = _fieldTree.MultiRead(Transaction, _value))
			{
				if (it.Seek(Slice.BeforeAllKeys) == false)
					yield break;
				do
				{
					it.CurrentKey.CopyTo(buffer);

					var termFreq = EndianBitConverter.Big.ToInt32(buffer, sizeof (long));
					var boost = EndianBitConverter.Big.ToSingle(buffer, sizeof (long) + sizeof (int));

					yield return new QueryMatch
					{
						DocumentId = EndianBitConverter.Big.ToInt64(buffer, 0),
						Score = Score(_weight, termFreq, boost * Boost)
					};
				} while (it.MoveNext());
			}

		}
	}
}