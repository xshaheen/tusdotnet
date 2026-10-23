#if pipelines

using System.IO.Pipelines;
using System.Threading.Tasks;
using tusdotnet.Adapters;
using tusdotnet.Extensions.Internal;
using tusdotnet.Helpers;

namespace tusdotnet.Models.PipeReaders
{
    internal static class GuardedPipeReaderFactory
    {
        internal static async Task<PipeReader> Create(ContextAdapter context)
        {
            PipeReader reader = new ClientDisconnectGuardedPipeReader(
                context.Request.BodyReader,
                context.ClientDisconnectGuard
            );

            reader = WrapWithFileLockLost(reader, context);
            reader = await WrapWithMaxSize(reader, context);
            reader = WrapWithChecksumInfo(reader, context);

            FileLockLostTokenRegistry.Register(reader, context.FileLockLostToken);

            return reader;
        }

        private static PipeReader WrapWithFileLockLost(PipeReader reader, ContextAdapter context)
        {
            // Wrap the disconnect guard directly, so the store and every outer guard see a lost lock as an exception.
            return context.FileLockLostToken.CanBeCanceled
                ? new FileLockLostGuardedPipeReader(reader, context.FileLockLostToken)
                : reader;
        }

        private static async Task<PipeReader> WrapWithMaxSize(
            PipeReader reader,
            ContextAdapter context
        )
        {
            var maxSizeData = await context.GetMaxReadSizeInfo();
            if (maxSizeData.MaxReadSize is not null)
            {
                reader = new MaxReadSizeGuardedPipeReader(
                    reader,
                    maxSizeData.PreviouslyRead,
                    maxSizeData.MaxReadSize.Value,
                    maxSizeData.SizeSource
                );
            }

            return reader;
        }

        private static PipeReader WrapWithChecksumInfo(PipeReader reader, ContextAdapter context)
        {
            if (
                context.StoreAdapter.Extensions.Checksum
                && context.ParsedRequest.UploadChecksum?.IsValid == true
            )
            {
                reader = new ChecksumAwarePipeReader(reader, context.ParsedRequest.UploadChecksum);
            }

            return reader;
        }
    }
}

#endif
