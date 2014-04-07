using Xunit;
using Corax.Utils;

namespace Corax.Tests
{

	public class HeapTests
	{
		readonly Heap<int> _heap = new Heap<int>(10);

		[Fact]
		public void CountStartsAtZero()
		{
			Assert.Equal(0, _heap.Count);
		}

		[Fact]
		public void AfterEnqueueCountIs1()
		{
			_heap.Enqueue(10);
			Assert.Equal(1, _heap.Count);
		}
		[Fact]
		public void AfterEnqueueMultipleItemsCountIs2()
		{
			_heap.Enqueue(10);
			_heap.Enqueue(7);
			Assert.Equal(2, _heap.Count);
		}

		[Fact]
		public void CanDequeueInOrder()
		{
			_heap.Enqueue(10);
			_heap.Enqueue(7);
			_heap.Enqueue(17);

			Assert.Equal(17, _heap.Dequeue());
			Assert.Equal(10, _heap.Dequeue());
			Assert.Equal(7, _heap.Dequeue());
		}


		[Fact]
		public void CanDequeueInOrder2()
		{
			_heap.Enqueue(10);
			_heap.Enqueue(7);
			_heap.Enqueue(127);
			_heap.Enqueue(2);
			_heap.Enqueue(7);
			_heap.Enqueue(17);

			Assert.Equal(127, _heap.Dequeue());
			Assert.Equal(17, _heap.Dequeue());
			Assert.Equal(10, _heap.Dequeue());
			Assert.Equal(7, _heap.Dequeue());
			Assert.Equal(7, _heap.Dequeue());
			Assert.Equal(2, _heap.Dequeue());
		}
	}
}