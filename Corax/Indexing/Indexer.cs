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
			DeleteDocument(id);
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

			var currentDocument = _bufferPool.Take(FullTextIndex.DocumentFieldSize);
			_usedBuffers.Add(currentDocument);

			EndianBitConverter.Big.CopyBytes(id, currentDocument, 0);

			_writeBatch.Delete(new Slice(currentDocument, sizeof(long)), "StoredFields");
			using (var snapshot = _parent.StorageEnvironment.CreateSnapshot())
			using (var it = snapshot.Iterate("Docs"))
			{
				if (it.Seek(new Slice(currentDocument)) == false)
					return; // not found, nothing to do
				do
				{
					var currentDocumentField = _bufferPool.Take(FullTextIndex.DocumentFieldSize);
					it.CurrentKey.CopyTo(currentDocumentField);

					var docId = EndianBitConverter.Big.ToInt64(currentDocumentField, 0);
					if (docId != id)
					{
						_bufferPool.Return(currentDocumentField);
						break;
					}

					_usedBuffers.Add(currentDocumentField);

					var fieldId = EndianBitConverter.Big.ToInt32(currentDocumentField, sizeof(long));

					_writeBatch.Delete(new Slice(currentDocumentField), "Docs");

					var size = (ushort)it.GetCurrentDataSize();

					var termBuffer = _bufferPool.Take(size);
					it.CreateReaderForCurrent().Read(termBuffer, 0, size);

					var termSlice = new Slice(termBuffer, size);

					var tree = GetTreeName(fieldId);
					using (var termIt = snapshot.MultiRead(tree, termSlice))
					{
						var currentFieldDocument = _bufferPool.Take(FullTextIndex.FieldDocumentSize);
						_usedBuffers.Add(currentFieldDocument);

						if (termIt.Seek(new Slice(currentFieldDocument)))
						{
							do
							{
								termIt.CurrentKey.CopyTo(currentDocumentField);

								if (EndianBitConverter.Big.ToInt64(currentDocument, 0) != id)
									break;

								var tempBuffer = _bufferPool.Take(termIt.CurrentKey.Size);
								_usedBuffers.Add(termBuffer);
								termIt.CurrentKey.CopyTo(tempBuffer);
								_writeBatch.MultiDelete(termSlice,new Slice(tempBuffer), tree);

							} while (termIt.MoveNext());
						}
					}
				} while (it.MoveNext());
			}
		}

		private void FlushCurrentDocument()
		{
			if (CurrentDocumentId <= 0)
				return;

			var docBuffer = _bufferPool.Take(sizeof(long));
			EndianBitConverter.Big.CopyBytes(CurrentDocumentId, docBuffer, 0);
			_usedBuffers.Add(docBuffer);
			
			if (_documentFields.Position != 0) // don't write if we don't have stored fields
			{
				_documentFields.Position = 0;
				_writeBatch.Add(new Slice(docBuffer), _documentFields, "StoredFields");
			}

			var countPerFieldName = new Dictionary<string, int>();
			foreach (var kvp in _currentTerms)
			{
				var field = kvp.Key.Item1;
				var tree = GetTreeName(field);
				var term = kvp.Key.Item2;
				var info = kvp.Value;
				var fieldBuffer = new byte[FullTextIndex.FieldDocumentSize];
				Buffer.BlockCopy(docBuffer, 0, fieldBuffer, 0, sizeof (long));
				EndianBitConverter.Big.CopyBytes(info.Freq, fieldBuffer, sizeof(long));
				EndianBitConverter.Big.CopyBytes(info.Boost, fieldBuffer, sizeof(long) + sizeof(int));
				var termSlice = new Slice(term.Buffer, (ushort)term.Size);
				_writeBatch.MultiAdd(termSlice, new Slice(fieldBuffer), tree);


				int fieldCount;
				countPerFieldName.TryGetValue(field, out fieldCount);
				fieldCount += 1;
				countPerFieldName[field] = fieldCount;

				var documentBuffer = _bufferPool.Take(FullTextIndex.DocumentFieldSize);
				_usedBuffers.Add(documentBuffer);
				Buffer.BlockCopy(docBuffer, 0, documentBuffer, 0, sizeof(long));
				EndianBitConverter.Big.CopyBytes(_parent.GetFieldNumber(field), documentBuffer, sizeof(long));
				EndianBitConverter.Big.CopyBytes(fieldCount, documentBuffer, sizeof(long) + sizeof(int));
				_writeBatch.Add(new Slice(documentBuffer), termSlice, "Docs");
			}

			_currentTerms.Clear();
			_documentFields = null;
			_binaryWriter.SetOutput(Stream.Null);
			CurrentDocumentId = -1;
		}

		private string GetTreeName(int fieldId)
		{
			return GetTreeName(_parent.GetFieldName(fieldId));
		}

		private string GetTreeName(string field)
		{
			string tree;
			if (_fieldTreesCache.TryGetValue(field, out tree) == false)
			{
				tree = "@fld_" + field;
				_fieldTreesCache[field] = tree;
			}
			return tree;
		}

		public void Flush()
		{
			FlushCurrentDocument();
			_parent.StorageEnvironment.Writer.Write(_writeBatch);
			
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