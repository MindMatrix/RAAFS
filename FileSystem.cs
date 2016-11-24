using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace RAAFS
{
    public interface IFileSystem
    {
        Stream Get(string file);
        Task<int> Put(string file, Stream stream);
    }

    public class FileCollection
    {
        private const int MAX_FILES_PER_COLLECTION = 23040;
        private const int AQUIRE_LOCK_TIMEOUT = 30000;
        //private const int AQUIRE_LOCK_TIMEOUT = 1000;

        private string _root;
        private int _totalFiles;
        private NamedReaderWriterLockSlim<string> _locks = new NamedReaderWriterLockSlim<string>();

        public Guid Key { get; private set; }
        public bool IsFull { get { return _totalFiles >= MAX_FILES_PER_COLLECTION; } }
        public int FileCount { get { return _totalFiles; } }

        public FileCollection(string root, Guid key)
        {
            _root = Path.Combine(root, key.ToString());
            Key = key;

            if (!Directory.Exists(_root))
                Directory.CreateDirectory(_root);

        }

        public void Increment()
        {
            _totalFiles++;
        }

        public async Task<int> Put(string file, Stream stream, Action afterWrite)
        {
            using (_locks.LockWrite(file, AQUIRE_LOCK_TIMEOUT))
            {
                var path = Path.Combine(_root, file);
                using (var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None))
                {
                    var amount = 0;
                    var read = 0;
                    var buffer = new byte[4096];
                    while ((read = await stream.ReadAsync(buffer, 0, buffer.Length)) > 0)
                    {
                        amount += read;
                        await fs.WriteAsync(buffer, 0, read);
                    }

                    afterWrite?.Invoke();
                    return amount;
                }
            }
        }

        public Stream Get(string file)
        {
            var path = Path.Combine(_root, file);

            var lck = _locks.LockRead(file, AQUIRE_LOCK_TIMEOUT);
            try
            {
                if (!File.Exists(path))
                {
                    lck.Dispose();
                    return null;
                }

                var fi = new FileInfo(path);
                return new ReadFileStream(fi, lck);
            }
            catch
            {
                lck.Dispose();
                throw;
            }
        }
    }

    public class FileSystem : IFileSystem
    {
        private const int MAX_CONCURRENCY = 8;

        private readonly ILogger<FileSystem> _logger;
        private readonly ConcurrentDictionary<string, Guid> _fileLookup = new ConcurrentDictionary<string, Guid>();
        private readonly ConcurrentDictionary<Guid, FileCollection> _fileCollections = new ConcurrentDictionary<Guid, FileCollection>();
        private readonly BlockingCollection<FileCollection> _writeableCollections = new BlockingCollection<FileCollection>();

        private readonly string _path;
        private readonly IOptions<FSOptions> _options;

        public FileSystem(ILogger<FileSystem> logger, IOptions<FSOptions> options)
        {
            _logger = logger;
            _options = options;

            _path = _options.Value.Root;
            if (!_path.EndsWith("\\"))
                _path += "\\";

            if (!Directory.Exists(_path))
                Directory.CreateDirectory(_path);

            var dirs = Directory.GetDirectories(_path);
            Parallel.ForEach(dirs, dir =>
            {
                var name = Path.GetFileName(dir);
                var key = Guid.Parse(name);
                var collection = new FileCollection(_path, key);

                _logger.LogInformation("Reading collection {0}", key);
                foreach (var file in System.IO.Directory.GetFiles(dir))
                {
                    var filename = System.IO.Path.GetFileName(file);
                    collection.Increment();

                    var fe = filename;
                    if (!_fileLookup.TryAdd(fe, collection.Key))
                        _logger.LogError("Failed to add {0} to collection {1}", fe, key);
                }

                _logger.LogInformation("Found {0} files in collection {1}", collection.FileCount, key);

                if (!collection.IsFull)
                {
                    _logger.LogInformation("Enabling write collection {0}", key);
                    _writeableCollections.Add(collection);
                }

                _fileCollections.TryAdd(key, collection);
            });

            while (_writeableCollections.Count < MAX_CONCURRENCY)
            {
                var collection = new FileCollection(_path, Guid.NewGuid());
                _logger.LogInformation("Creating write collection {0}", collection.Key);
                _writeableCollections.Add(new FileCollection(_path, collection.Key));
                _fileCollections.TryAdd(collection.Key, collection);
            }

        }

        public Stream Get(string file)
        {
            _logger.LogInformation("Get {0}", file);

            Guid collectionKey;
            if (!_fileLookup.TryGetValue(file, out collectionKey))
            {
                _logger.LogWarning("File not found {0}", file);
                return null; //not found
            }

            FileCollection collection;
            if (!_fileCollections.TryGetValue(collectionKey, out collection))
            {
                _logger.LogError("Collection not found {0}", collectionKey);
                return null; //collection missing, fatal error
            }

            try
            {
                var stream = collection.Get(file);
                if (stream == null)
                {
                    _logger.LogError("File missing from collection {0}/{1}", collectionKey, file);
                    return null; //file mapped to collection but missing on hdd
                }
                return stream;
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex.ToString());
            }

            return null;
        }

        public async Task<int> Put(string file, Stream stream)
        {
            _logger.LogInformation("Put {0}", file);

            Guid collectionKey;
            if (!_fileLookup.TryGetValue(file, out collectionKey))
            {
                _logger.LogInformation("Creating {0}", file);
                var collection = _writeableCollections.Take();
                return await collection.Put(file, stream, () =>
                {
                    collection.Increment();
                    _fileLookup.TryAdd(file, collection.Key);
                    
                    //check if we filled up a writeable collection
                    if (!collection.IsFull)
                        _writeableCollections.Add(collection);
                    else
                    {
                        collection = new FileCollection(_path, Guid.NewGuid());
                        _logger.LogInformation("Creating write collection {0}", collection.Key);
                        _writeableCollections.Add(new FileCollection(_path, collection.Key));
                        _fileCollections.TryAdd(collection.Key, collection);
                    }
                });
            }
            else
            {
                _logger.LogInformation("Replacing {0}", file);

                FileCollection collection;
                if (!_fileCollections.TryGetValue(collectionKey, out collection))
                {
                    _logger.LogError("Collection not found {0}", collectionKey);
                    return -1; //collection missing, fatal error
                }

                return await collection.Put(file, stream, null);
            }
        }
    }
}