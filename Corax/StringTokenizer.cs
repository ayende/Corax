using System.IO;

namespace Corax
{
	public class StringTokenizer
	{
		private bool _quoted;
		private TextReader _reader;

		public StringTokenizer(TextReader reader, int maxBufferSize = 256)
		{
			_reader = reader;
			Buffer = new char[maxBufferSize];
		}

		public char[] Buffer { get; private set; }

		public int Size { get; private set; }

		public int Line { get; private set; }

		public int Pos { get; private set; }

		private bool BufferFull
		{
			get { return Buffer.Length == Size; }
		}

		public bool Next()
		{
			Size = 0;
			char curr = '\0';
			while (true)
			{
				char prev = curr;
				int r = _reader.Read();
				Pos++;
				if (r == -1) // EOF
				{
					if (_quoted && Size > 0)
					{
						// we have an unterminated string, so we will ignore the quote, instead of erroring
						_reader = new StringReader(new string(Buffer, 0, Size));
						_quoted = false;
						Size = 0;
						curr = '\0';
						continue;
					}
					return Size > 0;
				}

				curr = (char) r;
				if (curr == '\r' || curr == '\n')
				{
					Pos = 0;
					if (prev != '\r' || curr != '\n')
					{
						Line++; // only move to new line if it isn't the \n in a \r\n pair
					}
					if (_quoted)
					{
						AppendToBuffer(curr);
						if (BufferFull)
						{
							return true;
						}
					}
					else if (Size > 0)
						return true;
					continue;
				}
				if (char.IsWhiteSpace(curr))
				{
					if (_quoted) // for a quoted string, we will continue until the end of the string
					{
						AppendToBuffer(curr);
						if (BufferFull)
						{
							return true;
						}
					}
					else if (Size > 0) // if we have content before, we will return this token
						return true;
					continue;
				}
				if (curr == '"')
				{
					if (_quoted == false)
					{
						_quoted = true;
						if (Size > 0)
							return true; // return the current token
						continue;
					}
					_quoted = false;
					return true;
				}

				if (char.IsPunctuation(curr))
				{
					// if followed by whitespace, ignore
					int next = _reader.Peek();
					if (next == -1 || char.IsWhiteSpace((char) next))
						continue;
				}

				AppendToBuffer(curr);
				if (BufferFull)
					return true;
			}
		}

		private void AppendToBuffer(char curr)
		{
			Buffer[Size++] = curr;
		}
	}
}