using System;
using System.Collections.Concurrent;
using System.Threading;
using Corax.Indexing;
using Corax.Queries;
using Corax.Utils;
using Voron;
using Voron.Impl;

namespace Corax
{
	public class FullTextIndex : IDisposable
	{
		private long _lastDocumentId;
		private int _lastFieldId;

		public IAnalyzer Analyzer { get; private set; }
		public Guid Id { get; private set; }
		public BufferPool BufferPool { get; private set; }
		public StorageEnvironment StorageEnvironment { get; private set; }

		private readonly ConcurrentDictionary<string, int> _fieldsIds = new ConcurrentDictionary<string, int>();

		public IndexingConventions Conventions { get; private set; }

		public FullTextIndex(StorageEnvironmentOptions options, IAnalyzer analyzer)
		{
			Analyzer = analyzer;
			Conventions = new IndexingConventions();
			BufferPool = new BufferPool();
			StorageEnvironment = new StorageEnvironment(options);

			using (var tx = StorageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
			{
				ReadMetadata(tx);
				ReadLastDocumentId(tx);
				ReadFields(tx);

				tx.Commit();
			}
		}

		public int GetFieldNumber(string field)
		{
			int num;
			if (_fieldsIds.TryGetValue(field, out num))
				return num;

			lock (_fieldsIds)
			{
				if (_fieldsIds.TryGetValue(field, out num))
					return num;

				_lastFieldId++;

				var wb = new WriteBatch();
				wb.Add(field, BitConverter.GetBytes(_lastFieldId), "Fields");
				StorageEnvironment.Writer.Write(wb);

				_fieldsIds.TryAdd(field, _lastFieldId);

				return _lastFieldId;
			}
		}

		private void ReadMetadata(Transaction tx)
		{
			var metadata = StorageEnvironment.CreateTree(tx, "$metadata");
			var idVal = metadata.Read(tx, "id");
			if (idVal == null)
			{
				Id = Guid.NewGuid();
				metadata.Add(tx, "id", Id.ToByteArray());
			}
			else
			{
				Id = new Guid(idVal.Reader.ReadBytes(16));
			}
		}

		private void ReadFields(Transaction tx)
		{
			var fields = StorageEnvironment.CreateTree(tx, "Fields");
			using (var it = fields.Iterate(tx))
			{
				if (it.Seek(Slice.BeforeAllKeys))
				{
					do
					{
						var field = it.CurrentKey.ToString();
						var id = it.CreateReaderForCurrent().ReadInt32();
						_fieldsIds.TryAdd(field, id);
						_lastFieldId = Math.Max(_lastFieldId, id);
					} while (it.MoveNext());
				}
			}
		}

		private void ReadLastDocumentId(Transaction tx)
		{
			var docs = StorageEnvironment.CreateTree(tx, "Documents");
			using (var it = docs.Iterate(tx))
			{
				_lastDocumentId = it.Seek(Slice.AfterAllKeys) == false ? 0 : it.CurrentKey.ToInt64();
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
			if (StorageEnvironment != null)
				StorageEnvironment.Dispose();
		}
	}
}