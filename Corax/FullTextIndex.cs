using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Corax.Indexing;
using Corax.Queries;
using Corax.Utils;
using Voron;
using Voron.Impl;
using Voron.Trees;
using Voron.Util.Conversion;

namespace Corax
{
    public class FullTextIndex : IDisposable
    {
        private long _lastDocumentId;

        public IAnalyzer Analyzer { get; private set; }
        public Guid Id { get; private set; }
        public BufferPool BufferPool { get; private set; }
        public StorageEnvironment StorageEnvironment { get; private set; }

        public IndexingConventions Conventions { get; private set; }

        public long NumberOfDocuments
        {
            get
            {
                using(var tx = StorageEnvironment.NewTransaction(TransactionFlags.Read))
                {
                    return tx.ReadTree("docs").State.EntriesCount;
                }
            }
        }

        public long NumberOfDeletes
        {
            get
            {
                using (var tx = StorageEnvironment.NewTransaction(TransactionFlags.Read))
                {
                    return tx.ReadTree("deletes").State.EntriesCount;
                }
            }
        }

        private Task _backgroundCompaction;
        private CancellationTokenSource _cts = new CancellationTokenSource();

        public FullTextIndex(StorageEnvironmentOptions options, IAnalyzer analyzer)
        {
            Analyzer = analyzer;
            Conventions = new IndexingConventions();
            BufferPool = new BufferPool();
            StorageEnvironment = new StorageEnvironment(options);

            using (var tx = StorageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
            {
                StorageEnvironment.CreateTree(tx, "@terms", keysPrefixing: true);
                StorageEnvironment.CreateTree(tx, "deletes", keysPrefixing: true);
                var docs = StorageEnvironment.CreateTree(tx, "docs", keysPrefixing: true);

                var metadata = StorageEnvironment.CreateTree(tx, "$metadata");
                var idVal = metadata.Read("id");
                if (idVal == null)
                {
                    Id = Guid.NewGuid();
                    metadata.Add("id", Id.ToByteArray());
                }
                else
                {
                    int _;
                    Id = new Guid(idVal.Reader.ReadBytes(16, out _));
                }
                using (var it = docs.Iterate())
                {
                    _lastDocumentId = it.Seek(Slice.AfterAllKeys) == false ?
                        0 :
                        it.CurrentKey.CreateReader().ReadBigEndianInt64();
                }
                tx.Commit();
            }
        }


        internal long NextDocumentId()
        {
            return Interlocked.Increment(ref _lastDocumentId);
        }

        public Indexer CreateIndexer()
        {
            return new Indexer(this);
        }

        public Searcher CreateSearcher()
        {
            return new Searcher(this);
        }

        public void Dispose()
        {
            using (_cts)
            using (StorageEnvironment)
            {
                _cts.Cancel();
                var backgroundCompaction = _backgroundCompaction;
                if (backgroundCompaction != null)
                {
                    backgroundCompaction.Wait();
                }
            }
        }

        public void MaybeStartCompaction()
        {
            if (Conventions.AutoCompact == false)
                return;
            if (_backgroundCompaction != null)
            {
                if (_backgroundCompaction.IsCanceled || _backgroundCompaction.IsFaulted)
                {
                    var oldBackgroundCompaction = _backgroundCompaction;
                    if (Interlocked.CompareExchange(ref _backgroundCompaction, null, oldBackgroundCompaction) ==
                        oldBackgroundCompaction)
                        oldBackgroundCompaction.Wait();
                }
                return;
            }


            using (var tx = StorageEnvironment.NewTransaction(TransactionFlags.Read))
            {
                var deletedDocuments = tx.ReadTree("deletes").State.EntriesCount;
                var allDocuments = tx.ReadTree("docs").State.EntriesCount;
                if (deletedDocuments == 0)
                    return;
                var threshold = allDocuments / 5;
                if (deletedDocuments < threshold)
                    return;
            }

            var backgroundCompaction = new Task(RunCompaction);
            if (Interlocked.CompareExchange(ref backgroundCompaction, backgroundCompaction, null) != null)
                return;
            if (_cts.IsCancellationRequested == false)
                backgroundCompaction.Start();
        }

        public void RunCompaction()
        {
            while (_cts.IsCancellationRequested == false)
            {
                var docIds = new List<Slice>();
                var terms = new List<Tuple<Slice, string>>();
                using (var tx = StorageEnvironment.NewTransaction(TransactionFlags.Read))
                {
                    var deletes = tx.ReadTree("deletes");
                    using (var docIt = deletes.Iterate())
                    {
                        if (docIt.Seek(Slice.BeforeAllKeys) == false)
                            return; // we are done, no more deletes
                        do
                        {
                          docIds.Add(docIt.CurrentKey.Clone());
                        } while (docIt.MoveNext() && docIds.Count < 1024);
                    }

                    using (var it = StorageEnvironment.State.Root.Iterate())
                    {
                        it.RequiredPrefix = "@fld_";
                        if (it.Seek(it.RequiredPrefix))
                        {
                            do
                            {
                                var tree = tx.ReadTree(it.CurrentKey.ToString());
                                using (var termsIt = tree.Iterate())
                                {
                                    if (termsIt.Seek(Slice.BeforeAllKeys))
                                    {
                                        do
                                        {
                                            terms.Add(Tuple.Create(termsIt.CurrentKey.Clone(), tree.Name));
                                        } while (termsIt.MoveNext());                                        
                                    }
                                }
                                
                            } while (it.MoveNext());
                        }
                    }
                }

                var writeBatch = new WriteBatch();
                foreach (var docId in docIds)
                {
                    foreach (var term in terms)
                    {
                        writeBatch.MultiDelete(term.Item1, docId, term.Item2);
                    }
                    writeBatch.Delete(docId, "deletes");
                    writeBatch.Delete(docId, "docs");

                    if (writeBatch.Size() > 1024*1024*8)
                        break;
                }
                StorageEnvironment.Writer.Write(writeBatch);

            }
        }
    }
}