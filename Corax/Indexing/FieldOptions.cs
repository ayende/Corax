using System;
using System.IO;
using Voron.Impl;

namespace Corax.Indexing
{
	[Flags]
	public enum FieldOptions
	{
		None = 0,
		NoAnalyzer = 1,
	}
}