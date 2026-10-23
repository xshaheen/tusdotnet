using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace tusdotnet.Models
{
    /// <summary>
    /// Stops reading the request body once the file lock is lost, so a store that never checks the lock-lost token
    /// still stops writing. It cannot undo data the store wrote before the lock was lost. A store that must reject
    /// that data uses a conditional write against the storage.
    /// </summary>
    internal class FileLockLostGuardedReadOnlyStream : ReadOnlyStream
    {
        private readonly CancellationToken _lockLostToken;

        public FileLockLostGuardedReadOnlyStream(
            Stream backingStream,
            CancellationToken lockLostToken
        )
            : base(backingStream)
        {
            _lockLostToken = lockLostToken;
        }

        public override async Task<int> ReadAsync(
            byte[] buffer,
            int offset,
            int count,
            CancellationToken cancellationToken
        )
        {
            ThrowIfLockLost();

            int read;

            // Pass the lock-lost token to the inner read, so a read waiting on a slow client stops when the lock
            // is lost instead of when the next bytes arrive.
            using (
                var cts = CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken,
                    _lockLostToken
                )
            )
            {
                try
                {
                    read = await BackingStream.ReadAsync(buffer, offset, count, cts.Token);
                }
                catch (OperationCanceledException) when (_lockLostToken.IsCancellationRequested)
                {
                    throw new FileLockLostException();
                }
            }

            // Another request may own the file now, so data read after the lock was lost must not reach the store.
            ThrowIfLockLost();

            return read;
        }

        private void ThrowIfLockLost()
        {
            if (_lockLostToken.IsCancellationRequested)
            {
                throw new FileLockLostException();
            }
        }
    }
}
