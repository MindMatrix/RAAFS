using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace RAAFS
{
    public class NamedReaderWriterLockSlim<T>
    {
        private Dictionary<T, RefCounter> _locks = new Dictionary<T, RefCounter>();
        private const int TIMEOUT_MILLISECONDS = 5000;
        public IDisposable LockRead(T name)
        {
            return LockRead(name, TIMEOUT_MILLISECONDS);
        }

        public IDisposable LockRead(T name, int timeoutMilliseconds)
        {
            return WithLock(name, refCounter =>
            {
                if (!refCounter.EnterRead(timeoutMilliseconds))
                    throw new TimeoutException(String.Format("Timed out after {0}ms waiting to acquire read lock on '{1}' - possible deadlock", timeoutMilliseconds, name));
                return 0;
            }, refCounter =>
            {
                return refCounter.ExitRead();
            });
        }

        public IDisposable LockWrite(T name)
        {
            return LockWrite(name, TIMEOUT_MILLISECONDS);
        }

        public IDisposable LockWrite(T name, int timeoutMilliseconds)
        {
            return WithLock(name, refCounter =>
            {
                if (!refCounter.EnterWrite(timeoutMilliseconds))
                    throw new TimeoutException(String.Format("Timed out after {0}ms waiting to acquire write lock on '{1}' - possible deadlock", timeoutMilliseconds, name));
                return 0;
            }, refCounter =>
            {
                return refCounter.ExitWrite();
            });
        }

        private void WithUnlock(T name, Func<RefCounter, int> unlockAction)
        {
            Monitor.Enter(_locks);
            {
                RefCounter refCounter = null;
                _locks.TryGetValue(name, out refCounter);
                if (refCounter != null)
                {
                    if (0 == unlockAction(refCounter))
                    {
                        _locks.Remove(name);
                    }
                }
            }
            Monitor.Exit(_locks);
        }

        private IDisposable WithLock(T name, Func<RefCounter, int> lockAction, Func<RefCounter, int> unlockAction)
        {
            Monitor.Enter(_locks);
            RefCounter refCounter = null;
            _locks.TryGetValue(name, out refCounter);
            if (refCounter == null)
            {
                refCounter = new RefCounter();
                lockAction(refCounter);
                _locks.Add(name, refCounter);
                Monitor.Exit(_locks);
            }
            else
            {
                Monitor.Exit(_locks);
                lockAction(refCounter);
            }
            return new Token(() => WithUnlock(name, unlockAction));
        }

        private enum LockType
        {
            Read,
            Write,
            WriteUpgrade
        }

        class RefCounter
        {
            private const int MAX_READERS = 4;
            private ConcurrentExclusiveSchedulerPair _scheduler = new ConcurrentExclusiveSchedulerPair();

            private Semaphore _lock = new Semaphore(MAX_READERS, MAX_READERS);

            private ReaderWriterLockSlim RWLock = new ReaderWriterLockSlim();
            private int _refcounter = 0;

            public bool EnterRead(int timeout)
            {
                Interlocked.Increment(ref _refcounter);
                if(!_lock.WaitOne(timeout))
                {
                    Interlocked.Decrement(ref _refcounter);
                    return false;
                }

                return true;
            }

            public bool EnterWrite(int timeout)
            {
                Interlocked.Increment(ref _refcounter);
                lock (_lock) //just makes sure 2 writes aren't draining at once
                {
                    var acquired = 0;
                    for (int i = 0; i < MAX_READERS; i++)
                    {
                        if (_lock.WaitOne(timeout))
                            acquired++;
                        else
                        {
                            _lock.Release(acquired);
                            Interlocked.Decrement(ref _refcounter);
                            return false;
                        }
                    }
                }

                return true;
            }

            public int ExitRead()
            {
                _lock.Release();
                return Interlocked.Decrement(ref _refcounter);
            }

            public int ExitWrite()
            {
                _lock.Release(MAX_READERS);
                return Interlocked.Decrement(ref _refcounter);
            }

            //public bool EnterLock(LockType type, int timeout)
            //{
            //    Interlocked.Increment(ref _refcounter);

            //    if (type == LockType.Read)
            //        return RWLock.TryEnterReadLock(timeout);
            //    else if (type == LockType.WriteUpgrade)
            //        return RWLock.TryEnterUpgradeableReadLock(timeout);

            //    return RWLock.TryEnterWriteLock(timeout);
            //}

            //public int ExitLock(LockType type)
            //{
            //    if (type == LockType.Read)
            //        RWLock.ExitReadLock();
            //    else if (type == LockType.WriteUpgrade)
            //        RWLock.ExitUpgradeableReadLock();
            //    else
            //        RWLock.ExitWriteLock();

            //    return Interlocked.Decrement(ref _refcounter);
            //}
        }

        class Token : IDisposable
        {
            private readonly Action _fn;

            public Token(Action fn)
            {
                _fn = fn;
            }
            public void Dispose()
            {
                _fn();
            }
        }
    }
}