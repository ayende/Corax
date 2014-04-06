using System.Collections.Concurrent;

namespace Corax.Utils
{
	public class BufferPool
	{
		private readonly ConcurrentDictionary<long, ConcurrentQueue<byte[]>> _buffers =
			new ConcurrentDictionary<long, ConcurrentQueue<byte[]>>();

		public byte[] Take(int size)
		{
			var nearestPowerOfTwo = Voron.Util.Utils.NearestPowerOfTwo(size);
			var queue = _buffers.GetOrAdd(nearestPowerOfTwo, CreateQueue);
			byte[] buffer;
			if (queue.TryDequeue(out buffer))
				return buffer;
			return new byte[nearestPowerOfTwo];
		}

		public void Return(byte[] buffer)
		{
			_buffers.GetOrAdd(buffer.Length, CreateQueue).Enqueue(buffer);
		}

		private static ConcurrentQueue<byte[]> CreateQueue(long _)
		{
			return new ConcurrentQueue<byte[]>();
		}
	}
}