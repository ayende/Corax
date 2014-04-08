using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
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

		private readonly BufferPool _bufferPool;

		private readonly Dictionary<string, string> _fieldTreesCache = new Dictionary<string, string>();
		private readonly List<byte[]> _usedBuffers = new List<byte[]>();

		public long CurrentDocumentId { get; private set; }

		public bool AutoFlush { get; set; }

		public void UpdateDocument(long id)
		{
			
			SetCurrentDocument(id);
		}

		private void SetCurrentDocument(long id)
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

		public void NewDocument()
		{
			SetCurrentDocument(_parent.NextDocumentId());
		}

		public void DeleteDocument(long id)
		{
		}

		private void FlushCurrentDocument()
		{
			if (CurrentDocumentId <= 0)
				return;
			_documentFields.Position = 0;

			var docBuffer = _bufferPool.Take(sizeof (long));
			EndianBitConverter.Big.CopyBytes(CurrentDocumentId, docBuffer, 0);
			_writeBatch.Add(new Slice(docBuffer), _documentFields, "Documents");
			_usedBuffers.Add(docBuffer);
		
			const int size = sizeof(long) // document id
			                 + sizeof(int) // term freq in doc
			                 + sizeof(float); // boost val

			foreach (var kvp in _currentTerms)
			{
				var field = kvp.Key.Item1;
				string tree;
				if (_fieldTreesCache.TryGetValue(field, out tree) == false)
				{
					tree = "@" + field;
					_fieldTreesCache[field] = tree;
				}
				var term = kvp.Key.Item2;
				var info = kvp.Value;
				var valBuffer = _bufferPool.Take(size);
				_usedBuffers.Add(valBuffer);
				Buffer.BlockCopy(docBuffer, 0, valBuffer, 0, sizeof (long));
				EndianBitConverter.Big.CopyBytes(info.Freq, valBuffer, sizeof(long));
				EndianBitConverter.Big.CopyBytes(info.Boost, valBuffer, sizeof(long) + sizeof(int));

				_writeBatch.MultiAdd(new Slice(term.Buffer, (ushort)term.Size), new Slice(valBuffer), tree);
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