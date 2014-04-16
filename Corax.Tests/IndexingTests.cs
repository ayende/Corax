using System.Linq;
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
					indexer.NewIndexEntry();
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
					indexer.NewIndexEntry();

					indexer.AddField("Name", "Oren Eini");

					indexer.Flush();
				}
			}
		}

		[Fact]
		public void WillFilterStopWords()
		{
			using (var fti = new FullTextIndex(StorageEnvironmentOptions.CreateMemoryOnly(), new DefaultAnalyzer()))
			{
				using (var indexer = fti.CreateIndexer())
				{
					indexer.NewIndexEntry();

					indexer.AddField("Name", "Oren and Ayende");

					indexer.Flush();
				}

				using (var searcher = fti.CreateSearcher())
				{
					Assert.Empty(searcher.Query(new TermQuery("Name", "and")));
				}
			}
		}

		[Fact]
		public void CanQueryUsingSingleTerm()
		{
			using (var fullTextIndex = new FullTextIndex(StorageEnvironmentOptions.CreateMemoryOnly(), new DefaultAnalyzer()))
			{
				using (var indexer = fullTextIndex.CreateIndexer())
				{
					indexer.NewIndexEntry();

					indexer.AddField("Name", "Oren Eini");

					indexer.AddField("Email", "ayende@ayende.com");

					indexer.NewIndexEntry();

					indexer.AddField("Name", "Arava Eini");

					indexer.AddField("Email", "arava@houseof.dog");

					indexer.Flush();
				}

				using (var searcher = fullTextIndex.CreateSearcher())
				{
					Assert.Equal(1, searcher.Query(new TermQuery("Name", "oren")).Count());
					Assert.Equal(1, searcher.Query(new TermQuery("Name", "rahien")).Count());
				}
			}
		}


		[Fact]
		public void CanDelete()
		{
			using (var fullTextIndex = new FullTextIndex(StorageEnvironmentOptions.CreateMemoryOnly(), new DefaultAnalyzer()))
			{
				using (var indexer = fullTextIndex.CreateIndexer())
				{
					indexer.NewIndexEntry();

					indexer.AddField("Name", "Oren Eini");

					indexer.NewIndexEntry();

					indexer.AddField("Name", "Ayende Rahien");

					indexer.Flush();
				}

				using (var searcher = fullTextIndex.CreateSearcher())
				{
					Assert.Equal(1, searcher.Query(new TermQuery("Name", "oren")).Count());
					Assert.Equal(1, searcher.Query(new TermQuery("Name", "rahien")).Count());
				}

				using (var indexer = fullTextIndex.CreateIndexer())
				{
					indexer.DeleteDocument(1);

					indexer.Flush();
				}

				using (var searcher = fullTextIndex.CreateSearcher())
				{
					Assert.Equal(0, searcher.Query(new TermQuery("Name", "oren")).Count());
					Assert.Equal(1, searcher.Query(new TermQuery("Name", "rahien")).Count());
				}

				using (var indexer = fullTextIndex.CreateIndexer())
				{
					indexer.DeleteDocument(2);

					indexer.Flush();
				}

				using (var searcher = fullTextIndex.CreateSearcher())
				{
					Assert.Equal(0, searcher.Query(new TermQuery("Name", "oren")).Count());
					Assert.Equal(0, searcher.Query(new TermQuery("Name", "rahien")).Count());
				}

			}
		}

		[Fact]
		public void CanQueryAndUpdate()
		{
			using (var fullTextIndex = new FullTextIndex(StorageEnvironmentOptions.CreateMemoryOnly(), new DefaultAnalyzer()))
			{
				using (var indexer = fullTextIndex.CreateIndexer())
				{
					indexer.NewIndexEntry(); // doc 1

					Assert.Equal(1L, indexer.CurrentDocumentId);

					indexer.AddField("Name", "Oren Eini");

					indexer.Flush();
				}

				using (var searcher = fullTextIndex.CreateSearcher())
				{
					Assert.Equal(1, searcher.Query(new TermQuery("Name", "oren")).Count());
				}

				using (var indexer = fullTextIndex.CreateIndexer())
				{
					indexer.UpdateIndexEntry(1); // doc 1

					Assert.Equal(1L, indexer.CurrentDocumentId);

					indexer.AddField("Name", "Ayende Rahien");

					indexer.Flush();
				}

				using (var searcher = fullTextIndex.CreateSearcher())
				{
					Assert.Equal(0, searcher.Query(new TermQuery("Name", "oren")).Count());
					Assert.Equal(1, searcher.Query(new TermQuery("Name", "ayende")).Count());
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
					indexer.NewIndexEntry();

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
					indexer.NewIndexEntry();

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