using BookSample.Data.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BookSample.Data.Model
{
    class Person : ReplicaItemBase, IPerson
    {
        internal Person(BookRepository repos, ReplicaItemId id, string name)
            : base(repos, id)
        {
            this._id.ItemType = ReplicaItemType.Person;
            this._name = name;
        }

        internal Person(BookRepository repos, Person source)
            : base(repos, source == null ? ReplicaItemId.Empty : source._id)
        {
            this._id.ItemType = ReplicaItemType.Person;
            copyItem(source);
            this._readOnly = false;
        }

        internal override void copyItem(ReplicaItemBase src)
        {
            Person source = (Person)src;
            copyItemBaseValues(source);
            if (source != null)
            {
                this._name = source._name;
            }
            else
            {
                this._name = String.Empty;
            }
            OnPropertyChanged(null);
        }

        private string _name;
        public string Name
        {
            get { return _name; }
            set { SetProperty(ref _name, value); }
        }
    }
}
