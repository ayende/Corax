using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Corax.Utils;
using Voron;
using Voron.Impl;

namespace Corax.Indexing
{
	public class Indexer : IDisposable
	{
		private readonly FullTextIndex _parent;
		private ITokenSource _source;
		private readonly IAnalyzer _analyzer;

		private WriteBatch _writeBatch = new WriteBatch();
        private readonly List<byte[]> _toBeFreed = new List<byte[]>(); 

		private readonly BufferPool _bufferPool;
	    private Slice _currentDocumentIdSlice;

	    public Indexer(FullTextIndex parent)
		{
			_parent = parent;
			_analyzer = _parent.Analyzer;
			_bufferPool = _parent.BufferPool;
	        FlushThresholdBytes = 1024*1024*8;
            AutoFlush = true;
		}

		public long CurrentDocumentId { get; private set; }

		public bool AutoFlush { get; set; }

        public long FlushThresholdBytes { get; set; }

		public void NewIndexEntry()
		{
            if (AutoFlush && _writeBatch.Size() > FlushThresholdBytes)
                Flush();

            CurrentDocumentId = _parent.NextDocumentId();
		    var sliceWriter = new SliceWriter(8);
		    sliceWriter.WriteBigEndian(CurrentDocumentId);
		    _currentDocumentIdSlice = sliceWriter.CreateSlice();
		    _writeBatch.Add(_currentDocumentIdSlice, Stream.Null, "docs");
		}

		public void DeleteIndexEntry(long id)
		{
            var sliceWriter = new SliceWriter(8);
            sliceWriter.WriteBigEndian(id);
            _currentDocumentIdSlice = sliceWriter.CreateSlice();

		    _writeBatch.Add(_currentDocumentIdSlice, Stream.Null, "deletes");
		}

		public void Flush()
		{
			_parent.StorageEnvironment.Writer.Write(_writeBatch);
            _writeBatch.Dispose();
            foreach (var b in _toBeFreed)
		    {
		        _bufferPool.Return(b);
		    }
            _toBeFreed.Clear();

            _parent.MaybeStartCompaction();

		    _writeBatch = new WriteBatch();
		}

		public void Index(string field, string indexedValue)
		{
		    Index(field, indexedValue == null ? null : new StringReader(indexedValue));
		}

	    public void Index(string field, TextReader indexedValue)
		{
            if (field == null) throw new ArgumentNullException("field");
	        if (field.Length > 256) throw new ArgumentException("field name cannot exceed 256 characters", "field");
            if (indexedValue == null) throw new ArgumentNullException("indexedValue");

            var treeName = "@fld_" + field;
            
            _source = _analyzer.CreateTokenSource(field, _source);
            _source.SetReader(indexedValue);
            while (_source.Next())
            {
                if (_analyzer.Process(field, _source) == false)
                    continue;

                var byteCount = Encoding.UTF8.GetByteCount(_source.Buffer, 0, _source.Size);
                if (byteCount > 256)
                    throw new IOException("Cannot index a term that is greater than 256 bytes, but got a term with " +
                                          byteCount + " bytes");
                var bytes = _bufferPool.Take(byteCount);
                _toBeFreed.Add(bytes);
                Encoding.UTF8.GetBytes(_source.Buffer, 0, _source.Size, bytes, 0);

                _writeBatch.MultiAdd(new Slice(bytes, (ushort) byteCount), _currentDocumentIdSlice, treeName);

            }
		}

		public void Dispose()
		{
            if(AutoFlush)
                Flush();
            _writeBatch.Dispose();
            foreach (var b in _toBeFreed)
            {
                _bufferPool.Return(b);
            }
            _toBeFreed.Clear();
		}
	}
}