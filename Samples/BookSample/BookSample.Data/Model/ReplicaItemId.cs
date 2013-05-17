using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BookSample.Data.Model
{
    class ReplicaItemId
    {
        public ReplicaItemType ItemType { get; set; }
        public long RowId { get; set; }
        public long CreationReplicaLocalId { get; set; }
        public long CreationTickCount { get; set; }
        public long ModificationReplicaLocalId { get; set; }
        public long ModificationTickCount { get; set; }

        public ReplicaItemId()
        {
        }

        public ReplicaItemId(ReplicaItemId source)
        {
            if (source != null)
            {
                ItemType = source.ItemType;
                RowId = source.RowId;
                CreationReplicaLocalId = source.CreationReplicaLocalId;
                CreationTickCount = source.CreationTickCount;
                ModificationReplicaLocalId = source.ModificationReplicaLocalId;
                ModificationTickCount = source.ModificationTickCount;
            }
        }


        private static ReplicaItemId _empty = new ReplicaItemId();
        public static ReplicaItemId Empty
        {
            get
            {
                return _empty;
            }
        }

        public override bool Equals (object obj)
        {
            //       
            // See the full list of guidelines at
            //   http://go.microsoft.com/fwlink/?LinkID=85237  
            // and also the guidance for operator== at
            //   http://go.microsoft.com/fwlink/?LinkId=85238
            //

            if (obj == null || GetType() != obj.GetType()) 
            {
                return false;
            }

            return (this == (ReplicaItemId)obj);
        }
    
        // override object.GetHashCode
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public static bool operator ==(ReplicaItemId a, ReplicaItemId b)
        {
            // If both are null, or both are same instance, return true.
            if (System.Object.ReferenceEquals(a, b))
            {
                return true;
            }

            // If one is null, but not both, return false.
            if (((object)a == null) || ((object)b == null))
            {
                return false;
            }


            if (a.ItemType != b.ItemType)
                return false;
            if (a.RowId != b.RowId)
                return false;
            if (a.CreationReplicaLocalId != b.CreationReplicaLocalId)
                return false;
            if (a.CreationTickCount != b.CreationTickCount)
                return false;
            if (a.ModificationReplicaLocalId != b.ModificationReplicaLocalId)
                return false;
            if (a.ModificationTickCount != b.ModificationTickCount)
                return false;
            return true;
        }

        public static bool operator !=(ReplicaItemId a, ReplicaItemId b)
        {
            return !(a == b);
        }
    }

    public enum ReplicaItemType
    {
        Unknown = 0,
        Book = 1, 
        Person = 2, 
    }
}
