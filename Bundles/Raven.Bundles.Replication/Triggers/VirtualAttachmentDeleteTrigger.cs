//-----------------------------------------------------------------------
// <copyright file="VirtualAttachmentDeleteTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Threading;
using Newtonsoft.Json.Linq;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Triggers
{
    /// <summary>
    /// We can't allow real deletes when using replication, because
    /// then we won't have any way to replicate the delete. Instead
    /// we allow the delete but don't do actual delete, we replace it 
    /// with a delete marker instead
    /// </summary>
    public class VirtualAttachmentDeleteTrigger : AbstractAttachmentDeleteTrigger
    {
        readonly ThreadLocal<RavenJToken> deletedSource = new ThreadLocal<RavenJToken>();
        readonly ThreadLocal<RavenJToken> deletedVersion = new ThreadLocal<RavenJToken>();

        public override void OnDelete(string key)
        {
            if (ReplicationContext.IsInReplicationContext)
                return;

            var document = Database.GetStatic(key);
            if (document == null)
                return;
            deletedSource.Value = document.Metadata[ReplicationConstants.RavenReplicationSource];
            deletedVersion.Value = document.Metadata[ReplicationConstants.RavenReplicationVersion];
        }

        public override void AfterDelete(string key)
        {
            if (ReplicationContext.IsInReplicationContext)
                return;
        	var metadata = new RavenJObject
        	{
        		{"Raven-Delete-Marker", true},
        		{ReplicationConstants.RavenReplicationParentSource, deletedSource.Value},
        		{ReplicationConstants.RavenReplicationParentVersion, deletedVersion.Value}
        	};
            deletedVersion.Value = null;
            deletedSource.Value = null;
            Database.PutStatic(key, null, new byte[0], metadata);
        }
    }
}