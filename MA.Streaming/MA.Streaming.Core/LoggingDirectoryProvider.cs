using MA.Common.Abstractions;

namespace MA.Streaming.Core;

public class LoggingDirectoryProvider : ILoggingDirectoryProvider
{
    private readonly string loggingDirectory;

    public LoggingDirectoryProvider(string loggingDirectory)
    {
        this.loggingDirectory = loggingDirectory;
    }

    public string Provide()
    {
        return this.loggingDirectory;
    }
}