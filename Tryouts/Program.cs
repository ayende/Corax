using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using Corax;
using Corax.Indexing;
using Voron;

namespace Tryouts
{
	class Program
	{
		static void Main(string[] args)
		{
			using (var fti = new FullTextIndex(StorageEnvironmentOptions.ForPath("index"), new DefaultAnalyzer()))
			{
				var tasks = new List<Task>();
				var queue = new BlockingCollection<string>(10*1000);
				for (int i = 0; i < 1; i++)
				{
					tasks.Add(Task.Run(() =>
					{
						int indexerCount = 0;
						using (var indexer = fti.CreateIndexer())
						{
							indexer.AutoFlush = false;
							while (queue.IsCompleted == false)
							{
								string line;
								try
								{
									line = queue.Take();
								}
								catch (InvalidOperationException)
								{
									break;
								}
								if (line == null)
									break;

								if (++indexerCount%(50*1000) == 0)
								{
									indexer.Flush();
								}

								indexer.NewIndexEntry();
								indexer.AddField("Title", line);


							}
							indexer.Flush();
						}
					}));	
				}

				var sp = Stopwatch.StartNew();
				int count = 0;
				using (var reader = new StreamReader(new GZipStream(File.OpenRead("titles.gz"),CompressionMode.Decompress)))
				{
					while (true)
					{
						var line = reader.ReadLine();
						if (line == null)
							break;

						queue.Add(line);

						if (++count%50000 == 0)
						{
							Console.WriteLine("{0,10:#,#}: {1}", count, line);
						}
					}
					queue.CompleteAdding();
					Console.WriteLine("Reading  {0:#,#} in {1}", count, sp.Elapsed);
				}

				while (Task.WaitAll(tasks.ToArray(), 1000) == false)
				{
					Console.Write("\r{0,10:#,#;;0}				",queue.Count);
				}

				Console.WriteLine("\rTotal " + sp.Elapsed);
			}

		}
	}
}
