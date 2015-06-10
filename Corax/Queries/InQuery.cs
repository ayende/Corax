//using System;
//using System.Collections.Generic;
//using System.Linq;
//using Voron;
//using Voron.Trees;
//using Voron.Util.Conversion;

//namespace Corax.Queries
//{
//    public class InQuery : Query
//    {
//        protected readonly List<string> Values;
//        protected readonly string Field;
//        private Tree _fieldTree;
//        private float _weight;
//        private readonly byte[] _fieldDocumentBuffer = new byte[FullTextIndex.FieldDocumentSize];

//        public InQuery(string field, params string[] values)
//        {
//            if (field == null) throw new ArgumentNullException("field");
//            if (values == null) throw new ArgumentNullException("values");

//            Field = field;
//            Values = new List<string>(values);
//        }

//        protected override void Init()
//        {
//            _fieldTree = Transaction.ReadTree("@fld_" + Field);
//            if (_fieldTree == null)
//                return;

//            var termFreqInDocs = _fieldTree.State.EntriesCount;
//            var numberOfDocs = Transaction.ReadTree("$metadata").Read("docs").Reader.ReadLittleEndianInt64();

//            var idf = Index.Conventions.Idf(termFreqInDocs, numberOfDocs);
//            _weight = idf*idf;

//            Values.Sort();
//        }

//        public override IEnumerable<QueryMatch> Execute()
//        {
//            if (Values.Count == 0 || _fieldTree == null)
//                return Enumerable.Empty<QueryMatch>();

//            var result = SingleQueryMatch(Values[0]);

//            for (var i = 1; i < Values.Count; i++)
//            {
//                var right = SingleQueryMatch(Values[i]);
//                result = JoinSubqueries(result, right);
//            }

//            return result;
//        }

//        protected virtual IEnumerable<QueryMatch> JoinSubqueries(IEnumerable<QueryMatch> left, IEnumerable<QueryMatch> right)
//        {
//            return left.Union(right, QueryMatchComparer.Instance);
//        }

//        public override string ToString()
//        {
//            return string.Format("@in<{0}>: ({1})", Field, string.Join(", ", Values));
//        }

//        private IEnumerable<QueryMatch> SingleQueryMatch(string value)
//        {
//            using (var it = _fieldTree.MultiRead(value))
//            {
//                if (it.Seek(Slice.BeforeAllKeys) == false)
//                    yield break; // yield break;
//                do
//                {
//                    it.CurrentKey.CopyTo(_fieldDocumentBuffer);

//                    var termFreq = EndianBitConverter.Big.ToInt32(_fieldDocumentBuffer, sizeof(long));
//                    var boost = EndianBitConverter.Big.ToSingle(_fieldDocumentBuffer, sizeof(long) + sizeof(int));

//                    yield return new QueryMatch
//                    {
//                        DocumentId = EndianBitConverter.Big.ToInt64(_fieldDocumentBuffer, 0),
//                        Score = Score(_weight, termFreq, boost*Boost)
//                    };
//                } while (it.MoveNext());
//            }
//        }
//    }
//}