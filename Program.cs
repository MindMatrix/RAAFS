using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using System.Collections.Concurrent;
using System.Threading;
using Microsoft.Extensions.Options;
using System.Net.Http;

namespace RAAFS
{
    public class FSOptions
    {
        public string Root { get; set; }
    }


    public class Program
    {
        public static string GetKey(Guid key, int altkey, int cookie)
        {
            return string.Format("{0}_{1:X4}_{2:X4}", key, altkey, cookie);
        }

        public static void Main(string[] args)
        {
            ////setup our DI
            //var serviceProvider = new ServiceCollection()
            //    .AddLogging()
            //    .AddOptions()
            //    .Configure<FSOptions>(x => { x.Root = Path.Combine(Directory.GetCurrentDirectory(), "data"); })
            //    .AddSingleton<ITest, Test>()
            //    .AddScoped<IFileSystem, FileSystem>()
            //    .BuildServiceProvider();



            ////configure console logging
            //serviceProvider
            //    .GetService<ILoggerFactory>()
            //    .AddConsole(LogLevel.Debug);

            //var fs = serviceProvider.GetService<IFileSystem>();
            //var logger = serviceProvider.GetService<ILoggerFactory>();

            //MainAsync(fs, logger.CreateLogger("Main")).GetAwaiter().GetResult();
            //Console.WriteLine("Done");
            //Console.Read();
            //////service.Get("test");

            ////var tasks = new List<Task>();
            ////for (int i = 0; i < 50; i++)
            ////{
            ////    tasks.Add(Task.Run(() => ));
            ////}

            ////Task.WaitAll(tasks.ToArray());
            ////Console.WriteLine("Done");
            //////var service = serviceProvider.GetService<ITest>();
            //////service.DoSomething();
            ////Console.Read();

            var host = new WebHostBuilder()
                .UseKestrel()
                .UseContentRoot(Directory.GetCurrentDirectory())
                .UseIISIntegration()
                .UseStartup<Startup>()
                .Build();


            var handle = new ManualResetEvent(false);
            Task.WaitAll(
                Task.Run(() =>
                {
                    host.Run();
                    handle.Set();
                }),
                Task.Run(async () =>
                {
                    while (!handle.WaitOne(0))
                    {
                        var key = Guid.NewGuid();
                        var r = new Random(key.GetHashCode());

                        var size = r.Next(10000, 100000);
                        var buffer = new byte[size];
                        r.NextBytes(buffer);

                        var client = new HttpClient();
                        await client.PostAsync("http://localhost:5000/files/" + key.ToString(), new ByteArrayContent(buffer));

                        var result = await client.GetAsync("http://localhost:5000/files/" + key.ToString());
                        using (var ms = new MemoryStream(size))
                        {
                            await result.Content.CopyToAsync(ms);

                            ms.Seek(0, SeekOrigin.Begin);

                            if (buffer.Length != ms.Length)
                                throw new Exception();

                            var i = 0;
                            while (ms.Position < size)
                            {
                                if (buffer[i++] != ms.ReadByte())
                                    throw new Exception();
                            }
                        }
                    }
                })
            );
        }

        public static async Task MainAsync(IFileSystem fs, ILogger logger)
        {
            var path = @"C:\thebluebookignore - Copy\000be0e9-0dc6-456f-8fa0-8d173bfd8c51.eml";
            var tasks = new List<Task>();
            for (int i = 0; i < 50; i++)
            {
                tasks.Add(Task.Run(() => fs.Put(GetKey(Guid.Empty, 0, 0), File.OpenRead(path))));
            }

            using (var stream = fs.Get(GetKey(Guid.Empty, 0, 0)))
            {

            }

            for (int i = 0; i < 50; i++)
            {
                tasks.Add(Task.Run(async () =>
                    {
                        using (var stream = fs.Get(GetKey(Guid.Empty, 0, 0)))
                        {
                            if (stream != null)
                            {
                                var read = 0;
                                var amount = 0;
                                var buffer = new byte[4096];
                                while ((read = await stream.ReadAsync(buffer, 0, buffer.Length)) > 0)
                                {
                                    amount += read;
                                }

                                logger.LogInformation("Read {0}", amount);
                            }
                            else
                            {
                                logger.LogWarning("File not found {0}", GetKey(Guid.Empty, 0, 0));
                            }
                        }
                    }
                ));
            }


            Task.WaitAll(tasks.ToArray());

            var wrote = await fs.Put(GetKey(Guid.Empty, 0, 0), File.OpenRead(path));
            logger.LogInformation("write {0}", wrote);
        }
    }
}
