using System.IO;

namespace Corax.Indexing
{
	public interface ITokenSource
	{
		char[] Buffer { get; }
		int Size { get; set; }
		
		int Line { get; }
		int Column { get; }

		int Position { get; set; }

		bool Next();

		void SetReader(TextReader reader);
	}
}