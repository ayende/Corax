using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Voron;
using Voron.Trees;
using Voron.Util.Conversion;

namespace Corax.Queries
{
	public class PhraseQuery : Query
	{
		private readonly string _field;
		private readonly string[] _values;
		private Tree _fieldTree;
		private Tree _positionsTree;
		private float _weight;
		private int _fieldNumber;

		public PhraseQuery(string field, params string[] phraseValues)
		{
			_field = field;
			_values = phraseValues;
		}

		public override string ToString()
		{
			return string.Format("{0}: {1}", _field, String.Join(" ", _values));
		}

		protected override void Init()
		{
			_fieldTree = Transaction.ReadTree("@fld_" + _field);
			if (_fieldTree == null)
				return;

			_positionsTree = Transaction.ReadTree("TermPositions");
			if (_positionsTree == null)
				return;

			_fieldNumber = Index.GetFieldNumber(_field);

			var termFreqInDocs = _fieldTree.State.EntriesCount;
			var numberOfDocs = Transaction.ReadTree("$metadata").Read(Transaction, "docs").Reader.ReadInt64();

			var idf = Index.Conventions.Idf(termFreqInDocs, numberOfDocs);
			_weight = idf*idf; // square it
		}

		public override IEnumerable<QueryMatch> Execute()
		{
			if (_fieldTree == null || _positionsTree == null)
				yield break;

			var fieldDocumentBuffer = new byte[FullTextIndex.FieldDocumentSize];

			// This is not right. On the first run through I can get a document id, but the remaining values out of @fld_X
			// are irrelevant to me. Then need to verify that the rest of the terms are present in the same document, and
			// in the correct order. Querying @fld_X can get me the verification that the rest of the terms are present
			// but not in a very manageable manner. Querying TermPositions by the document id can get me all the terms in the doc but
			// then I won't know what term is what. There doesn't appear to be a straightforward way to reduce each field to its
			// field ids, to then look up the positions and verify the orders. Must be missing something...

			foreach (string value in _values)
			{
				using (var it = _fieldTree.MultiRead(Transaction, value))
				{
					if (it.Seek(Slice.BeforeAllKeys) == false)
						continue;
					do
					{
						it.CurrentKey.CopyTo(fieldDocumentBuffer);

						var termFreq = EndianBitConverter.Big.ToInt32(fieldDocumentBuffer, sizeof(long));
						var boost = EndianBitConverter.Big.ToSingle(fieldDocumentBuffer, sizeof(long) + sizeof(int));

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
}
