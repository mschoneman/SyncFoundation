using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BookSample.Data.Model
{
    class ReposItemId
    {
        public ReposItemType ItemType { get; set; }
        public long RowId { get; set; }
        public long CreationRepositoryLocalId { get; set; }
        public long CreationTickCount { get; set; }
        public long ModificationRepositoryLocalId { get; set; }
        public long ModificationTickCount { get; set; }

        public ReposItemId()
        {
        }

        public ReposItemId(ReposItemId source)
        {
            if (source != null)
            {
                ItemType = source.ItemType;
                RowId = source.RowId;
                CreationRepositoryLocalId = source.CreationRepositoryLocalId;
                CreationTickCount = source.CreationTickCount;
                ModificationRepositoryLocalId = source.ModificationRepositoryLocalId;
                ModificationTickCount = source.ModificationTickCount;
            }
        }


        private static ReposItemId _empty = new ReposItemId();
        public static ReposItemId Empty
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

            return (this == (ReposItemId)obj);
        }
    
        // override object.GetHashCode
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public static bool operator ==(ReposItemId a, ReposItemId b)
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
            if (a.CreationRepositoryLocalId != b.CreationRepositoryLocalId)
                return false;
            if (a.CreationTickCount != b.CreationTickCount)
                return false;
            if (a.ModificationRepositoryLocalId != b.ModificationRepositoryLocalId)
                return false;
            if (a.ModificationTickCount != b.ModificationTickCount)
                return false;
            return true;
        }

        public static bool operator !=(ReposItemId a, ReposItemId b)
        {
            return !(a == b);
        }
    }

    public enum ReposItemType
    {
        Unknown = 0,
        Book = 1, 
        Person = 2, 
    }
}
