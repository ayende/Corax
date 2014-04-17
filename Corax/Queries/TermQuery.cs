using System;
using System.Collections.Generic;
using Voron;
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
			if (field == null) throw new ArgumentNullException("field");
			if (value == null) throw new ArgumentNullException("value");
			_field = field;
			_value = value;
		}

		public override string ToString()
		{
			return string.Format("{0}: {1}", _field, _value);
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
		}

		public override IEnumerable<QueryMatch> Execute()
		{
			if (_fieldTree == null)
				yield break;

			var fieldDocumentBuffer = new byte[FullTextIndex.FieldDocumentSize];
			using (var it = _fieldTree.MultiRead(Transaction, _value))
			{
				if (it.Seek(Slice.BeforeAllKeys) == false)
					yield break;
				do
				{
					it.CurrentKey.CopyTo(fieldDocumentBuffer);

					var termFreq = EndianBitConverter.Big.ToInt32(fieldDocumentBuffer, sizeof (long));
					var boost = EndianBitConverter.Big.ToSingle(fieldDocumentBuffer, sizeof (long) + sizeof (int));

					yield return new QueryMatch
					{
						DocumentId = EndianBitConverter.Big.ToInt64(fieldDocumentBuffer, 0),
						Score = Score(_weight, termFreq, boost * Boost)
					};
				} while (it.MoveNext());
			}

		}
	}
}