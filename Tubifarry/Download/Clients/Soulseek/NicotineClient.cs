using NLog;
using NzbDrone.Common.Disk;
using NzbDrone.Core.Configuration;
using NzbDrone.Core.Localization;
using NzbDrone.Core.RemotePathMappings;
using Tubifarry.Indexers.Soulseek;

namespace Tubifarry.Download.Clients.Soulseek;

/// <summary>
/// Settings for <see cref="NicotineClient"/>: identical to the Slskd settings, only the defaults differ.
/// </summary>
public class NicotineProviderSettings : SlskdProviderSettings
{
    public NicotineProviderSettings() => BaseUrl = NicotineIndexer.DefaultBaseUrl;
}

/// <summary>
/// Downloads through Nicotine+ using the Tubifarry API plugin (NicotinePlugin/tubifarry_api),
/// which serves the slskd transfer endpoints on top of the Nicotine+ download queue.
/// </summary>
public class NicotineClient(
    ISlskdDownloadManager manager,
    ISlskdApiClient apiClient,
    IConfigService configService,
    IDiskProvider diskProvider,
    IRemotePathMappingService remotePathMappingService,
    ILocalizationService localizationService,
    Logger logger)
    : SlskdClientBase<NicotineProviderSettings>(manager, apiClient, configService, diskProvider, remotePathMappingService, localizationService, logger)
{
    public override string Name => "Nicotine+";
}
