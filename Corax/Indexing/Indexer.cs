using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using Corax.Indexing.Filters;
using Corax.Utils;
using Voron;
using Voron.Impl;
using Voron.Util.Conversion;

namespace Corax.Indexing
{
	public class Indexer : IDisposable
	{
		private readonly FullTextIndex _parent;
		private ITokenSource _source;
		private readonly IAnalyzer _analyzer;
		private readonly ReusableBinaryWriter _binaryWriter = new ReusableBinaryWriter();

		private WriteBatch _writeBatch = new WriteBatch();
		private readonly Dictionary<Tuple<string, ArraySegmentKey<byte>>, TermInfo> _currentTerms =
			new Dictionary<Tuple<string, ArraySegmentKey<byte>>, TermInfo>();

		private readonly BufferPool _bufferPool;

		private readonly Dictionary<string, string> _fieldTreesCache = new Dictionary<string, string>();
		private readonly List<byte[]> _usedBuffers = new List<byte[]>();
		private int addedDocsCounts;
		private int deletedDocsCount;

		public class TermInfo
		{
			public int Freq;
			public float Boost;
		}

		private int _currentTermCount;
		private int _currentStoredCount;
		private Stream _documentFields;

		public Indexer(FullTextIndex parent)
		{
			_parent = parent;
			_analyzer = _parent.Analyzer;
			_bufferPool = _parent.BufferPool;
			AutoFlush = true;
		}

		public long CurrentDocumentId { get; private set; }

		public bool AutoFlush { get; set; }

		public void UpdateIndexEntry(long id)
		{
			SetCurrentIndexEntry(id);
		}

		private void SetCurrentIndexEntry(long id)
		{
			if (CurrentDocumentId > 0)
			{
				FlushCurrentDocument();
				if (AutoFlush)
				{
					if (_currentStoredCount > 16384 || _currentTermCount > 16384)
					{
						Flush();
					}
				}
			}
			CurrentDocumentId = id;
			_documentFields = new BufferPoolMemoryStream(_bufferPool);
			_binaryWriter.SetOutput(_documentFields);
		}

		public void NewIndexEntry()
		{
			addedDocsCounts++;
			SetCurrentIndexEntry(_parent.NextDocumentId());
		}

		public void DeleteDocument(long id)
		{
			deletedDocsCount++;
		}

		private void FlushCurrentDocument()
		{
			if (CurrentDocumentId <= 0)
				return;
			_documentFields.Position = 0;

			var docBuffer = _bufferPool.Take(sizeof (long));
			EndianBitConverter.Big.CopyBytes(CurrentDocumentId, docBuffer, 0);
			_writeBatch.Add(new Slice(docBuffer), _documentFields, "StoredFields");
			_usedBuffers.Add(docBuffer);

			const int fieldDocumentSize =
				sizeof (long) +  // document id
				sizeof (int) + // term freq in doc
				sizeof (float); // boost val

			const int documentFieldSize = 
				sizeof(long) + // document id
				sizeof(int) + // field id
				sizeof(ushort); // field value counter

			var countPerFieldName = new Dictionary<string, ushort>();
			foreach (var kvp in _currentTerms)
			{
				var field = kvp.Key.Item1;
				string tree;
				if (_fieldTreesCache.TryGetValue(field, out tree) == false)
				{
					tree = "@fld_" + field;
					_fieldTreesCache[field] = tree;
				}
				var term = kvp.Key.Item2;
				var info = kvp.Value;
				var fieldBuffer = new byte[fieldDocumentSize];
				Buffer.BlockCopy(docBuffer, 0, fieldBuffer, 0, sizeof (long));
				EndianBitConverter.Big.CopyBytes(info.Freq, fieldBuffer, sizeof(long));
				EndianBitConverter.Big.CopyBytes(info.Boost, fieldBuffer, sizeof(long) + sizeof(int));
				var termSlice = new Slice(term.Buffer, (ushort)term.Size);
				_writeBatch.MultiAdd(termSlice, new Slice(fieldBuffer), tree);


				ushort fieldCount;
				countPerFieldName.TryGetValue(field, out fieldCount);
				fieldCount += 1;
				if (fieldCount == ushort.MaxValue)
					throw new InvalidOperationException("Too many terms for field " + field);

				countPerFieldName[field] = fieldCount;

				var documentBeffer = new byte[documentFieldSize];
				Buffer.BlockCopy(docBuffer, 0, documentBeffer, 0, sizeof(long));
				EndianBitConverter.Big.CopyBytes(_parent.GetFieldNumber(field), fieldBuffer, sizeof(long));
				EndianBitConverter.Big.CopyBytes(fieldCount, fieldBuffer, sizeof(long) + sizeof(int));
				_writeBatch.Add(new Slice(documentBeffer), termSlice, "@docs");
			}

			_currentTerms.Clear();
			_documentFields = null;
			_binaryWriter.SetOutput(Stream.Null);
			CurrentDocumentId = -1;
		}

		public void Flush()
		{
			FlushCurrentDocument();
			_parent.StorageEnvironment.Writer.Write(_writeBatch);
			Interlocked.Add(ref _parent.NumberOfDocuments, addedDocsCounts - deletedDocsCount);
			_writeBatch = new WriteBatch();
			foreach (var usedBuffer in _usedBuffers)
			{
				_bufferPool.Return(usedBuffer);
			}
			_usedBuffers.Clear();
		}

		public void AddField(string field, string indexedValue = null, string storedValue = null, FieldOptions options = FieldOptions.None)
		{
			AddField(field, indexedValue == null ? null : new StringReader(indexedValue), storedValue, options);
		}

		public void AddField(string field, TextReader indexedValue = null, string storedValue = null, FieldOptions options = FieldOptions.None)
		{
			if (storedValue != null)
			{
				var num = _parent.GetFieldNumber(field);
				_binaryWriter.Write(num);
				_binaryWriter.Write(storedValue);
				_currentStoredCount++;
			}

			if (indexedValue == null)
			{
				if (storedValue == null)
					throw new ArgumentException("It isn't meaningful to pass null for both indexedValue and storedValue");
				return;
			}

			_source = _analyzer.CreateTokenSource(field, _source);
			_source.SetReader(indexedValue);
			while (_source.Next())
			{
				if (options.HasFlag(FieldOptions.NoAnalyzer) == false)
				{
					if (_analyzer.Process(field, _source) == false)
						continue;
				}

				var byteCount = Encoding.UTF8.GetByteCount(_source.Buffer, 0, _source.Size);
				var bytes = _bufferPool.Take(byteCount);
				Encoding.UTF8.GetBytes(_source.Buffer, 0, _source.Size, bytes, 0);
				Debug.Assert(byteCount < ushort.MaxValue);
				_currentTermCount++;

				var key = Tuple.Create(field, new ArraySegmentKey<byte>(bytes, byteCount));

				TermInfo info;
				if (_currentTerms.TryGetValue(key, out info))
				{
					_bufferPool.Return(bytes);
				}
				else
				{
					_currentTerms[key] = info = new TermInfo {Boost = 1.0f};
					_usedBuffers.Add(bytes);
				}

				info.Freq++;
			}
		}

		public void Dispose()
		{
			_writeBatch.Dispose();
			if (_documentFields != null)
				_documentFields.Dispose();
			foreach (var usedBuffer in _usedBuffers)
			{
				_bufferPool.Return(usedBuffer);
			}
			_usedBuffers.Clear();
		}

		private class ReusableBinaryWriter : BinaryWriter
		{
			public void SetOutput(Stream stream)
			{
				OutStream = stream;
			}
		}
	}
}