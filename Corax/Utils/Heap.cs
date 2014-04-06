using System;

namespace Corax.Utils
{
	public class Heap<T>
	{
		private readonly Comparison<T> _comparer;
		private readonly T[] _values;

		public int Count { get; private set; }

		public Heap(int size, Comparison<T> comparer)
		{
			_comparer = comparer;
			_values = new T[size];
		}

		public void Enqueue(T value)
		{
			if (Count >= _values.Length)
				throw new InvalidOperationException("Heap full");
			var index = Count;
			Count++;
			_values[index] = value;

			BubbleUp(index);
		}

		private void BubbleUp(int index)
		{
			while (index > 0)
			{
				var parentIndex = (index - 1) / 2;
				if (_comparer(_values[index], _values[parentIndex]) < 0)
					break;
				var parent = _values[parentIndex];
				_values[parentIndex] = _values[index];
				_values[index] = parent;
				index = parentIndex;
			}
		}

		private void TrickleDown()
		{
			int index = 0;
			var childIndex = (index * 2) + 1;
			while (childIndex < Count)
			{
				if (childIndex + 1 < Count &&
					_comparer(_values[childIndex], _values[childIndex + 1]) < 0)
				{
					childIndex++;
				}
				var tmp = _values[index];
				_values[index] = _values[childIndex];
				_values[childIndex] = tmp;
				index = childIndex;
				childIndex = (index * 2) + 1;
			}
			BubbleUp(index);
		}

		public T Dequeue()
		{
			if (Count == 0)
				throw new InvalidOperationException("Heap empty");

			var val = _values[0];
			Count--;
			_values[0] = _values[Count];
			TrickleDown();
			_values[Count] = default(T);

			return val;
		}
	}
}