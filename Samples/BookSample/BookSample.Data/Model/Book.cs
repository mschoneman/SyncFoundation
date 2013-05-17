using BookSample.Data.Interfaces;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BookSample.Data.Model
{
    class Book : ReplicaItemBase, IBook
    {
        internal Book(BookRepository repos, ReplicaItemId id, string title)
            : base(repos, id)
        {
            this._id.ItemType = ReplicaItemType.Book;
            this._title = title;
        }

        internal Book(BookRepository repos, Book source)
            : base(repos, source == null ? ReplicaItemId.Empty : source._id)
        {
            this._id.ItemType = ReplicaItemType.Book;
            copyItem(source);
            this._readOnly = false;
        }

        internal override void copyItem(ReplicaItemBase src)
        {
            Book source = (Book)src;
            copyItemBaseValues(source);

            _authors = null;
            _readOnlyAuthors = null;

            if (source != null)
            {
                this._title = source._title;
                if (source._authors != null)
                {
                    _authors = new ObservableCollection<IPerson>();
                    _readOnlyAuthors = new ReadOnlyObservableCollection<IPerson>(_authors);

                    foreach (IPerson person in source._authors)
                        this._authors.Add(person);

                    _authors.CollectionChanged += aurthors_CollectionChanged;
                }

            }
            else
            {
                this._title = String.Empty;
            }
            OnPropertyChanged(null);
        }

        private string _title;
        public string Title
        {
            get { return _title; }
            set { SetProperty(ref _title, value); }
        }

        internal bool authorsChanged { get; private set; }
        private ObservableCollection<IPerson> _authors;
        private ReadOnlyObservableCollection<IPerson> _readOnlyAuthors;

        void aurthors_CollectionChanged(object sender, System.Collections.Specialized.NotifyCollectionChangedEventArgs e)
        {
            IsModified = true;
            authorsChanged = true;
        }

        private void loadAuthors()
        {
            if (_authors == null)
            {
                _authors = _repos.loadBookAuthors(_id.RowId);
                _authors.CollectionChanged += aurthors_CollectionChanged;
                _readOnlyAuthors = new ReadOnlyObservableCollection<IPerson>(_authors);
            }
        }

        public IList<IPerson> Authors
        {
            get
            {
                loadAuthors();
                return IsReadOnly ? (IList<IPerson>)_readOnlyAuthors : (IList<IPerson>)_authors;
            }
        }
    }
}
