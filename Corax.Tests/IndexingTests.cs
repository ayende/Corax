using Corax.Indexing;
using Corax.Queries;
using Voron;
using Xunit;

namespace Corax.Tests
{
	public class IndexingTests
	{
		[Fact]
		public void CanCreateAndDispose()
		{
			using (new FullTextIndex(StorageEnvironmentOptions.CreateMemoryOnly(), new DefaultAnalyzer()))
			{
				
			}
		}

		[Fact]
		public void CanCreateAndDisposeIndexer()
		{
			using (var fti = new FullTextIndex(StorageEnvironmentOptions.CreateMemoryOnly(), new DefaultAnalyzer()))
			{
				using (fti.CreateIndexer())
				{
					
				}
			}
		}

		[Fact]
		public void CanIndexEmptyDocument()
		{
			using (var fti = new FullTextIndex(StorageEnvironmentOptions.CreateMemoryOnly(), new DefaultAnalyzer()))
			{
				using (var indexer = fti.CreateIndexer())
				{
					indexer.NewDocument();
					indexer.Flush();
				}
			}
		}

		[Fact]
		public void CanIndexSingleValue()
		{
			using (var fti = new FullTextIndex(StorageEnvironmentOptions.CreateMemoryOnly(), new DefaultAnalyzer()))
			{
				using (var indexer = fti.CreateIndexer())
				{
					indexer.NewDocument();

					indexer.AddField("Name", "Oren Eini");

					indexer.Flush();
				}
			}
		}

		[Fact]
		public void CanQueryUsingSingleTerm()
		{
			using (var fti = new FullTextIndex(StorageEnvironmentOptions.CreateMemoryOnly(), new DefaultAnalyzer()))
			{
				using (var indexer = fti.CreateIndexer())
				{
					indexer.NewDocument();

					indexer.AddField("Name", "Oren Eini");

					indexer.Flush();
				}

				using (var searcher = fti.CreateSearcher())
				{
					Assert.Empty(searcher.Query(new TermQuery("Name", "Arava")));
				}
			}
		}


		[Fact]
		public void CanQueryUsingMissingTerm()
		{
			using (var fti = new FullTextIndex(StorageEnvironmentOptions.CreateMemoryOnly(), new DefaultAnalyzer()))
			{
				using (var indexer = fti.CreateIndexer())
				{
					indexer.NewDocument();

					indexer.AddField("Name", "Oren Eini");

					indexer.Flush();
				}

				using (var searcher = fti.CreateSearcher())
				{
					Assert.Empty(searcher.Query(new TermQuery("Name", "Arava")));
				}
			}
		}


		[Fact]
		public void CanQueryUsingMissingField()
		{
			using (var fti = new FullTextIndex(StorageEnvironmentOptions.CreateMemoryOnly(), new DefaultAnalyzer()))
			{
				using (var indexer = fti.CreateIndexer())
				{
					indexer.NewDocument();

					indexer.AddField("Name", "Oren Eini");

					indexer.Flush();
				}

				using (var searcher = fti.CreateSearcher())
				{
					Assert.Empty(searcher.Query(new TermQuery("Foo", "Arava")));

				}
			}
		}

		[Fact]
		public void CanQueryOnEmptyindex()
		{
			using (var fti = new FullTextIndex(StorageEnvironmentOptions.CreateMemoryOnly(), new DefaultAnalyzer()))
			{
				using (var searcher = fti.CreateSearcher())
				{
					Assert.Empty(searcher.Query(new TermQuery("Foo", "Arava")));
				}
			}
		} 
	}
}