using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using BookSample.Data.Interfaces;


namespace BookSample.Data.Model
{
    abstract class ReposItemBase : IBaseItem
    {
        protected BookRepository _repository;
        protected bool _disposed;
        protected bool _modified;
        protected bool _readOnly = true;

        protected ReposItemId _id;

        public ReposItemBase(BookRepository repository, ReposItemId id)
        {
            _repository = repository;
            _id = new ReposItemId(id);
        }

        public ReposItemId Id
        {
            get
            {
                return _id;
            }
        }

        public bool IsModified
        {
            get
            {
                return _modified;
            }
            internal set
            {
                _modified = value;
            }
        }

        protected void copyItemBaseValues(ReposItemBase source)
        {
            if (source != null)
            {
                _id.RowId = source._id.RowId;
                _id.CreationRepositoryLocalId = source._id.CreationRepositoryLocalId;
                _id.CreationTickCount = source._id.CreationTickCount;
                _id.ModificationRepositoryLocalId = source._id.ModificationRepositoryLocalId;
                _id.ModificationTickCount = source._id.ModificationTickCount;
                _id.ItemType = source._id.ItemType;
            }
        }

        internal abstract void copyItem(ReposItemBase source);

        /// <summary>
        /// Gets a value indicating whether the <see cref="ReposItemBase"/> is read-only.
        /// </summary>
        /// <value>
        /// <b>true</b> if the <see cref="ReposItemBase"/> is read-only; otherwise <b>false</b>.
        /// </value>
        public bool IsReadOnly
        {
            get
            {
                return _readOnly;
            }
            internal set
            {
                _readOnly = value;
            }
        }


        #region Implementation of IDisposable

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <remarks>
        /// Use this method to close or release unmanaged resources such as files, streams, and handles held by an instance of the class that implements this interface. This method is, by convention, used for all tasks associated with freeing resources held by an object, or preparing an object for reuse.
        /// </remarks>
        public void Dispose()
        {
            if (!_disposed)
            {
                Dispose(true);
                GC.SuppressFinalize(this);
            }
        }

        /// <summary>
        /// Releases the unmanaged resources used by the <see cref="ReposItemBase"/> and optionally releases the managed resources.
        /// </summary>
        /// <param name="disposing"><b>true</b> to release both managed and unmanaged resources; <b>false</b> to release only unmanaged resources.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                // Free other state (managed objects)
            }
            // Free other state (unmanged objects)
            if (!_disposed)
            {
                //LibRepos.repos_release
            }
            // Set large fields to null
            _disposed = true;
        }

        /// <summary>
        /// Allows an <see cref="ReposItemBase" /> to attempt to free resources and perform other cleanup operations before the <b>ReposItemBase</b> is reclaimed by garbage collection.
        /// </summary>
        /// <remarks>
        /// In C#, finalizers are expressed using destructor syntax.
        /// </remarks>
        ~ReposItemBase()
        {
            // Simply call Dispose(false)
            Dispose(false);
        }

        #endregion

        protected bool SetProperty<T>(ref T storage, T value, [CallerMemberName] string propertyName = null)
        {
            if (IsReadOnly)
                throw new InvalidOperationException();

            if (Object.Equals(storage, value)) return false;

            storage = value;
            this.OnPropertyChanged(propertyName);
            return true;
        }


        public event PropertyChangedEventHandler PropertyChanged;
        // Create the OnPropertyChanged method to raise the event
        protected void OnPropertyChanged(string name = null)
        {
            _modified = true;
            PropertyChangedEventHandler handler = PropertyChanged;
            if (handler != null)
            {
                handler(this, new PropertyChangedEventArgs(name));
            }
        }
    }
}
