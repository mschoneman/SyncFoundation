using BookSample.Data.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BookSample.Data.Model
{
    class Person : ReposItemBase, IPerson
    {
        internal Person(BookRepository repository, ReposItemId id, string name)
            : base(repository, id)
        {
            this._id.ItemType = ReposItemType.Person;
            this._name = name;
        }

        internal Person(BookRepository repository, Person source)
            : base(repository, source == null ? ReposItemId.Empty : source._id)
        {
            this._id.ItemType = ReposItemType.Person;
            copyItem(source);
            this._readOnly = false;
        }

        internal override void copyItem(ReposItemBase src)
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
