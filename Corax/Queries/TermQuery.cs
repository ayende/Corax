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
        private Tree _deletes;

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

            _deletes = Transaction.ReadTree("deletes");

            var termFreqInDocs = _fieldTree.State.EntriesCount;
            var numberOfDocs = Transaction.ReadTree("docs").State.EntriesCount;

            var idf = Index.Conventions.Idf(termFreqInDocs, numberOfDocs);
            _weight = idf * idf;
        }

        public override IEnumerable<QueryMatch> Execute()
        {
            if (_fieldTree == null)
                yield break;

            using (var it = _fieldTree.MultiRead(_value))
            {
                if (it.Seek(Slice.BeforeAllKeys) == false)
                    yield break;
                do
                {
                    if (_deletes.ReadVersion(it.CurrentKey) != 0)
                        continue; // document was deleted
                    yield return new QueryMatch
                    {
                        DocumentId = it.CurrentKey.CreateReader().ReadBigEndianInt64(),
                        Score = 0f
                    };
                } while (it.MoveNext());
            }

        }
    }
}