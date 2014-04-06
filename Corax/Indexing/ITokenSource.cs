using System.IO;

namespace Corax.Indexing
{
	public interface ITokenSource
	{
		char[] Buffer { get; }
		int Size { get; set; }
		int Line { get; }
		int Pos { get; }
		bool Next();

		void SetReader(TextReader reader);
	}
}