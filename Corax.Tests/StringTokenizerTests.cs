using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

namespace Corax.Tests
{
	public class StringTokenizerTests
	{
		[Fact]
		public void OnEmptyStringWillReturnNothing()
		{
			Assert.Empty(Tokenize(string.Empty));
		}

		[Fact]
		public void WillGetSingleToken()
		{
			Assert.Equal("hello",Tokenize("hello").Single());
		}

		[Fact]
		public void CanHandleQuoteStrings()
		{
			Assert.Equal(new[] { "hello", "single token", "test"}, Tokenize("hello \"single token\" test").ToList());
		}

		[Fact]
		public void CanHandleMultiLineStrings()
		{
			Assert.Equal(new[] { "hello", "there" }, Tokenize("hello\r\nthere").ToList());
		}

		[Fact]
		public void CanHandleMultiLineQuotedStrings()
		{
			Assert.Equal(new[] { "hello", "\r\nthere dude", "nice", "isn't", "it" }, Tokenize("hello\"\r\nthere dude\"\r\n nice isn't it?").ToList());
		}

		[Fact]
		public void WillRecognizeEmails()
		{
			Assert.Equal("ayende@ayende.com", Tokenize("ayende@ayende.com").Single());
		}

		[Fact]
		public void WillGetMultipleTokenOnComma()
		{
			Assert.Equal(new[]{"hello", "there"}, Tokenize("hello, there").ToList());
		}

		[Fact]
		public void WillGetMultipleToken()
		{
			Assert.Equal(new[] { "hello" , "world"}, Tokenize("hello world").ToList());
		}

		[Fact]
		public void UnterminatedQuotedStringWillError()
		{
			Assert.Equal(new[]{"hello","there"},Tokenize("\"hello there").ToList());
		}

		public IEnumerable<string> Tokenize(string s)
		{
			var x = new StringTokenizer(new StringReader(s));
			while (x.Next())
			{
				yield return new String(x.Buffer, 0, x.Size);
			}
		}
	}
}