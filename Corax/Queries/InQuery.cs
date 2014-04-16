using System.Collections.Generic;
using System.Linq;
using Voron;
using Voron.Trees;
using Voron.Util.Conversion;

namespace Corax.Queries
{
	//TODO: Use BooleanQuery With 'OR' ??
	public class InQuery : Query
	{
		private readonly string[] _values;
		private readonly string _field;
		private readonly TermQuery[] _termQueries ;
		private Tree _fieldTree;
		private int _fieldNumber;
		private float _weight;
		private string[] _sortedValues;

		public InQuery(string field, params string[] values)
		{
			_field = field;
			_values = values;
			_termQueries = new TermQuery[_values.Length];
		}

		protected override void Init()
		{
			_fieldTree = Transaction.ReadTree("@fld_" + _field);
			if (_fieldTree == null)
				return;

			_fieldNumber = Index.GetFieldNumber(_field);

			var termFreqInDocs = _fieldTree.State.EntriesCount;
			var numberOfDocs = Transaction.ReadTree("$metadata").Read(Transaction, "docs").Reader.ReadInt64();

			var idf = Index.Conventions.Idf(termFreqInDocs, numberOfDocs);
			_weight = idf*idf; // square it

			_sortedValues = _values.AsQueryable().OrderBy(x => x).ToArray(); 
			
		}

		public override IEnumerable<QueryMatch> Execute()
		{
			if (_values == null || _values.Length == 0)
			{
				yield break;
			}
			
			if (_fieldTree == null)
				yield break;

			foreach (var value in _sortedValues)
			{
				var fieldDocumentBuffer = new byte[FullTextIndex.FieldDocumentSize];
				using (var it = _fieldTree.MultiRead(Transaction, value))
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
							Score = Score(_weight, termFreq, boost*Boost)
						};
					} while (it.MoveNext());
				}
			}

		}
	}
}