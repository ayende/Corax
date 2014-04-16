using System;
using System.Collections.Concurrent;
using System.Threading;
using Corax.Indexing;
using Corax.Queries;
using Corax.Utils;
using Voron;
using Voron.Impl;
using Voron.Util.Conversion;

namespace Corax
{
	public class FullTextIndex : IDisposable
	{
		internal const int FieldDocumentSize =
					sizeof(long) +  // document id
					sizeof(int) + // term freq in doc
					sizeof(float); // boost val

		internal const int DocumentFieldSize =
			sizeof(long) + // document id
			sizeof(int) + // field id
			sizeof(int); // field value counter

		private long _lastDocumentId;
		private int _lastFieldId;

		public IAnalyzer Analyzer { get; private set; }
		public Guid Id { get; private set; }
		public BufferPool BufferPool { get; private set; }
		public StorageEnvironment StorageEnvironment { get; private set; }

		private readonly ConcurrentDictionary<string, int> _fieldsNamesToIds = new ConcurrentDictionary<string, int>();
		private readonly ConcurrentDictionary<int, string> _fieldIdsToName = new ConcurrentDictionary<int, string>();

		public IndexingConventions Conventions { get; private set; }

		public FullTextIndex(StorageEnvironmentOptions options, IAnalyzer analyzer)
		{
			Analyzer = analyzer;
			Conventions = new IndexingConventions();
			BufferPool = new BufferPool();
			StorageEnvironment = new StorageEnvironment(options);

			using (var tx = StorageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
			{
				StorageEnvironment.CreateTree(tx, "TermPositions");
				ReadMetadata(tx);
				ReadLastDocumentId(tx);
				ReadFields(tx);

				tx.Commit();
			}
		}


		public string GetFieldName(int fieldId)
		{
			string value;
			_fieldIdsToName.TryGetValue(fieldId, out value);
			return value;
		}

		public int GetFieldNumber(string field)
		{
			int num;
			if (_fieldsNamesToIds.TryGetValue(field, out num))
				return num;

			lock (_fieldsNamesToIds)
			{
				if (_fieldsNamesToIds.TryGetValue(field, out num))
					return num;

				_lastFieldId++;

				var wb = new WriteBatch();
				wb.Add(field, new Slice(BitConverter.GetBytes(_lastFieldId)), "FieldNames");
				StorageEnvironment.Writer.Write(wb);

				_fieldIdsToName.TryAdd(_lastFieldId, field);
				_fieldsNamesToIds.TryAdd(field, _lastFieldId);

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
				metadata.Add(tx, "docs", BitConverter.GetBytes(0L));
			}
			else
			{
				Id = new Guid(idVal.Reader.ReadBytes(16));
				NumberOfDocuments = metadata.Read(tx, "docs").Reader.ReadInt64();
			}
		}

		public long NumberOfDocuments { get; set; }

		private void ReadFields(Transaction tx)
		{
			var fields = StorageEnvironment.CreateTree(tx, "FieldNames");
			using (var it = fields.Iterate(tx))
			{
				if (it.Seek(Slice.BeforeAllKeys))
				{
					do
					{
						var field = it.CurrentKey.ToString();
						var id = it.CreateReaderForCurrent().ReadInt32();
						_fieldsNamesToIds.TryAdd(field, id);
						_fieldIdsToName.TryAdd(id, field);
						_lastFieldId = Math.Max(_lastFieldId, id);
					} while (it.MoveNext());
				}
			}
		}

		private void ReadLastDocumentId(Transaction tx)
		{
			var docs = StorageEnvironment.CreateTree(tx, "Docs");
			using (var it = docs.Iterate(tx))
			{
				if (it.Seek(Slice.AfterAllKeys) == false)
				{
					_lastDocumentId = 0;
					return;
				}
				var buffer = new byte[DocumentFieldSize];
				it.CurrentKey.CopyTo(buffer);
				_lastDocumentId = EndianBitConverter.Big.ToInt64(buffer, 0);
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