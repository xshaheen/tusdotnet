using System.IO;
using System.Threading.Tasks;
using tusdotnet.Adapters;
using tusdotnet.Extensions.Internal;
using tusdotnet.Helpers;
using tusdotnet.Models;
using tusdotnet.Models.Streams;

namespace tusdotnet.IntentHandlers
{
    internal static class GuardedStreamFactory
    {
        internal static async Task<Stream> Create(ContextAdapter context)
        {
            Stream guardedStream = new ClientDisconnectGuardedReadOnlyStream(
                context.Request.Body,
                context.ClientDisconnectGuard
            );

            guardedStream = WrapWithFileLockLost(guardedStream, context);
            guardedStream = await WrapWithMaxSize(guardedStream, context);
            guardedStream = WrapWithChecksumInfo(guardedStream, context);

            FileLockLostTokenRegistry.Register(guardedStream, context.FileLockLostToken);

            return guardedStream;
        }

        private static Stream WrapWithFileLockLost(Stream stream, ContextAdapter context)
        {
            // Wrap the disconnect guard directly, so the store and every outer guard see a lost lock as an exception.
            return context.FileLockLostToken.CanBeCanceled
                ? new FileLockLostGuardedReadOnlyStream(stream, context.FileLockLostToken)
                : stream;
        }

        private static async Task<Stream> WrapWithMaxSize(Stream stream, ContextAdapter context)
        {
            var maxSizeData = await context.GetMaxReadSizeInfo();
            if (maxSizeData.MaxReadSize is not null)
            {
                stream = new MaxReadSizeGuardedReadOnlyStream(
                    stream,
                    maxSizeData.PreviouslyRead,
                    maxSizeData.MaxReadSize.Value,
                    maxSizeData.SizeSource
                );
            }

            return stream;
        }

        private static Stream WrapWithChecksumInfo(Stream stream, ContextAdapter context)
        {
            if (
                context.StoreAdapter.Extensions.Checksum
                && context.ParsedRequest.UploadChecksum?.IsValid == true
            )
            {
                stream = new ChecksumAwareStream(stream, context.ParsedRequest.UploadChecksum);
            }

            return stream;
        }
    }
}
