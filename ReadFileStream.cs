using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace RAAFS
{
    public class ReadFileStream : Stream
    {
        private IDisposable _lck;
        private FileInfo _fileInfo;
        private FileStream _stream;

        public ReadFileStream(FileInfo fi, IDisposable lck)
        {
            _fileInfo = fi;
            _lck = lck;
            _stream = _fileInfo.Open(FileMode.Open, FileAccess.Read, FileShare.Read);
        }

        public override bool CanRead
        {
            get
            {
                return true;
            }
        }

        public override bool CanSeek
        {
            get
            {
                return true;
            }
        }

        public override bool CanWrite
        {
            get
            {
                return false;
            }
        }

        public override long Length
        {
            get
            {
                return _fileInfo.Length;
            }
        }

        public override long Position
        {
            get
            {
                return _stream.Position;
            }

            set
            {
                _stream.Position = value;
            }
        }

        public override void Flush()
        {
            _stream.Flush();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            return _stream.Read(buffer, offset, count);
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return _stream.ReadAsync(buffer, offset, count, cancellationToken);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return _stream.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            _stream.Write(buffer, offset, count);
        }

        protected override void Dispose(bool disposing)
        {
            if(_stream != null)
            {
                _stream.Dispose();
                _stream = null;
            }

            if(_lck != null)
            {
                _lck.Dispose();
                _lck = null;
            }
            base.Dispose(disposing);
        }
    }
}
