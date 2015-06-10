//using System;
//using System.Collections.Generic;
//using System.Linq;
//using Voron;
//using Voron.Trees;
//using Voron.Util.Conversion;

//namespace Corax.Queries
//{
//    public class PhraseQuery : InQuery
//    {
//        private readonly Slice[] _orderedValues;
//        private Tree _positionsTree;
//        private Tree _docsTree;
//        private byte[] _prefix;
//        private Slice _prefixSlice;
//        private Slice _maxKeySlice;
//        private byte[] _maxKey;
//        private int _fieldId;

//        public PhraseQuery(string field, params string[] values)
//            : base(field, values)
//        {
//            _orderedValues = values.Select(s => (Slice)s).ToArray();
//            Slop = 1;
//        }

//        public int Slop { get; set; }

//        public override string ToString()
//        {
//            return string.Format("{0}: \"{1}\"{2} ", Field,
//                String.Join(" ", Values),
//                Slop == 1 ? "" : "~" + Slop);
//        }

//        protected override void Init()
//        {
//            base.Init();

//            _positionsTree = Transaction.ReadTree("TermPositions");
//            _docsTree = Transaction.ReadTree("Docs");


//            _fieldId = Index.GetFieldNumber(Field);
//            _prefix = new byte[FullTextIndex.FieldDocumentSize];
//            _prefixSlice = new Slice(_prefix);

//            _maxKey = new byte[FullTextIndex.FieldDocumentSize];
//            _maxKeySlice = new Slice(_maxKey);
//        }

//        public override IEnumerable<QueryMatch> Execute()
//        {
//            var docsWithAllTerms = base.Execute();
//            if (_orderedValues.Length == 1)
//                return docsWithAllTerms;

//            return FindMatchesForPhrases(docsWithAllTerms);
//        }

//        private IEnumerable<QueryMatch> FindMatchesForPhrases(IEnumerable<QueryMatch> docsWithAllTerms)
//        {
//            foreach (var match in docsWithAllTerms)
//            {
//                var sort = GetTermPositionsFor(match.DocumentId);
//                if (sort == null)
//                    continue; // not a match

//                using (var enumerator = sort.GetEnumerator())
//                {
//                    int foundPos = 0;
//                    int lastPos = 0;
//                    while (enumerator.MoveNext())
//                    {
//                        var currentPos = enumerator.Current.Key;
//                        if (_orderedValues[foundPos].Equals(enumerator.Current.Value) == false)
//                        {
//                            if (foundPos != 0 && (lastPos + Slop < currentPos)) // we are past the slop we need, reset
//                            {
//                                foundPos = 0;
//                                lastPos = 0;
//                            }
//                            continue;
//                        }
//                        foundPos++;
//                        lastPos = currentPos;
//                        if (foundPos == _orderedValues.Length) // found all, done
//                        {
//                            yield return match;
//                            break;
//                        }
//                    }
//                }
//            }
//        }

//        private SortedList<int, Slice> GetTermPositionsFor(long documentId)
//        {
//            using (var posIt = _positionsTree.Iterate())
//            {
//                EndianBitConverter.Big.CopyBytes(documentId, _prefix, 0);
//                EndianBitConverter.Big.CopyBytes(_fieldId, _prefix, sizeof (long));

//                EndianBitConverter.Big.CopyBytes(documentId, _maxKey, 0);
//                EndianBitConverter.Big.CopyBytes(_fieldId, _maxKey, sizeof (long));
//                EndianBitConverter.Big.CopyBytes(int.MaxValue, _maxKey, sizeof (long) + sizeof (int));

//                posIt.MaxKey = _maxKeySlice;
//                if (posIt.Seek(_prefixSlice) == false)
//                    return null;

//                var sort = new SortedList<int, Slice>();
//                do
//                {
//                    var term = _docsTree.Read( posIt.CurrentKey);
//                    if (term == null)
//                        continue;

//                    var slice = term.Reader.AsSlice();
//                    var reader = posIt.CreateReaderForCurrent();
//                    while (reader.EndOfData == false)
//                    {
//                        sort.Add(reader.ReadLittleEndianInt32(), slice);
//                    }
//                } while (posIt.MoveNext());
//                return sort;
//            }
//        }

//        protected override IEnumerable<QueryMatch> JoinSubqueries(IEnumerable<QueryMatch> left, IEnumerable<QueryMatch> right)
//        {
//            return left.Intersect(right, QueryMatchComparer.Instance);
//        }
//    }
//}
