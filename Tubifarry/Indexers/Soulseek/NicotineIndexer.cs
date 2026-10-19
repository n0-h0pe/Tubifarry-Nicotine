using NLog;
using NzbDrone.Common.Http;
using NzbDrone.Core.Configuration;
using NzbDrone.Core.Download.History;
using NzbDrone.Core.History;
using NzbDrone.Core.Indexers;
using NzbDrone.Core.Parser;
using NzbDrone.Core.Queue;
using NzbDrone.Core.ThingiProvider;
using Tubifarry.Core.Telemetry;
using Tubifarry.Indexers.Soulseek.Search.Core;

namespace Tubifarry.Indexers.Soulseek
{
    /// <summary>
    /// Soulseek indexer backed by Nicotine+ through the Tubifarry API plugin (NicotinePlugin/tubifarry_api),
    /// which exposes the slskd-compatible endpoints used by <see cref="SlskdIndexer"/>.
    /// </summary>
    public class NicotineIndexer : SlskdIndexer
    {
        public const string DefaultBaseUrl = "http://localhost:5031";

        public override string Name => "Nicotine+";

        public NicotineIndexer(IHttpClient httpClient, Lazy<IIndexerFactory> indexerFactory, IIndexerStatusService indexerStatusService, ISlskdSearchChain slskdSearchChain, ISlskdItemsParser slskdItemsParser, IHistoryService historyService, IDownloadHistoryService downloadHistoryService, IQueueService queueService, IConfigService configService, IParsingService parsingService, ISentryHelper sentry, Logger logger)
          : base(httpClient, indexerFactory, indexerStatusService, slskdSearchChain, slskdItemsParser, historyService, downloadHistoryService, queueService, configService, parsingService, sentry, logger)
        {
        }

        public override IEnumerable<ProviderDefinition> DefaultDefinitions
        {
            get
            {
                foreach (ProviderDefinition definition in base.DefaultDefinitions)
                {
                    if (definition.Settings is SlskdSettings settings)
                        settings.BaseUrl = DefaultBaseUrl;
                    yield return definition;
                }
            }
        }
    }
}
