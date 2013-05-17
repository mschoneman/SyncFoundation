using BookSample.Data.Interfaces;
using BookSample.Data.Model;
using Community.CsharpSqlite.SQLiteClient;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BookSample.Data
{
    public class BookRepository
    {
        private string _filename;

        private ObservableCollection<IPerson> _allPeople;
        private ReadOnlyObservableCollection<IPerson> _allPeopleReadOnly;

        private ObservableCollection<IBook> _allBooks;
        private ReadOnlyObservableCollection<IBook> _allBooksReadOnly;

        public BookRepository(string filename)
        {
            bool existing = File.Exists(filename);
            string datadir = filename.Substring(0, filename.Length - Path.GetFileName(filename).Length);
            using (IDbConnection connection = DbFactory.CreateConnection())
            {
                string cs = string.Format("Version=3,busy_timeout=250,uri=file:{0}", filename);
                connection.ConnectionString = cs;
                connection.Open();

                IDbCommand command = connection.CreateCommand();
                command.CommandText = "PRAGMA foreign_keys = 1";
                command.ExecuteNonQuery();

                if (!existing)
                    initializeDatabase(connection);

                _filename = filename;
                loadAllPeople(connection);
                loadAllBooks(connection);
            }
        }


        private const string itemIdFieldsForSelect = "_rowid_ AS RowID, CreatedRepos, CreatedTickCount, ModifiedRepos, ModifiedTickCount";
        private const string itemIdFieldsAndForiegnKeysForCreate = "CreatedRepos INTEGER, CreatedTickCount INTEGER, ModifiedRepos INTEGER, ModifiedTickCount INTEGER, FOREIGN KEY(CreatedRepos) REFERENCES SyncRepositories(LocalReposID), FOREIGN KEY(ModifiedRepos) REFERENCES SyncRepositories(LocalReposID)";

        #region SQLite "DbProviderFactory"
        private class SqliteClientFactory
        {
            public static SqliteClientFactory Instance = null;
            public static object lockStatic = new object();

            private SqliteClientFactory()
            {
            }

            static SqliteClientFactory()
            {
                lock (lockStatic)
                {
                    if (Instance == null)
                        Instance = new SqliteClientFactory();
                }
            }

            public IDbCommand CreateCommand()
            {
                return new SqliteCommand();
            }

            public IDbConnection CreateConnection()
            {
                return new SqliteConnection();
            }

        }

        private SqliteClientFactory DbFactory
        {
            get
            {
                return SqliteClientFactory.Instance;
            }
        }
        #endregion

        #region Helper Methods

        internal IDbConnection createConnection()
        {
            if (_filename == null)
                return null;
            string cs = string.Format("Version=3,busy_timeout=500,uri=file:{0}", _filename);
            IDbConnection connection = DbFactory.CreateConnection();
            connection.ConnectionString = cs;
            connection.Open();

            IDbCommand command = connection.CreateCommand();
            command.CommandText = "PRAGMA foreign_keys = 1";
            command.ExecuteNonQuery();

            return connection;
        }

        private ReposItemId getReposItemIdFromDataReader(IDataReader reader)
        {
            ReposItemId id = new ReposItemId();
            id.RowId = Convert.ToInt64(reader["RowID"]);
            id.CreationRepositoryLocalId = Convert.ToInt64(reader["CreatedRepos"]);
            id.CreationTickCount = Convert.ToInt64(reader["CreatedTickCount"]);
            id.ModificationRepositoryLocalId = Convert.ToInt64(reader["ModifiedRepos"]);
            id.ModificationTickCount = Convert.ToInt64(reader["ModifiedTickCount"]);
            return id;
        }

        private void insertTombstone(IDbConnection connection, ReposItemId id)
        {
            IDbCommand command = connection.CreateCommand();
            command.CommandText = "INSERT INTO Tombstones(ItemType, CreatedRepos, CreatedTickCount, ModifiedRepos, ModifiedTickCount, DeletionDateTime) VALUES (@ItemType,@CreatedRepos,@CreatedTick,0,@TickCount,datetime('now'))";
            command.AddParameter("@ItemType", id.ItemType);
            command.AddParameter("@CreatedRepos", id.CreationRepositoryLocalId);
            command.AddParameter("@CreatedTick", id.CreationTickCount);
            command.AddParameter("@TickCount", incrementTickCount(connection, 0));
            command.ExecuteNonQuery();
        }

        internal long incrementTickCount(IDbConnection connection, long localRepositoryId)
        {
            IDbCommand command = connection.CreateCommand();
            command.CommandText = "SAVEPOINT UpdateTick";
            command.ExecuteNonQuery();
            try
            {
                command.CommandText = String.Format("UPDATE SyncRepositories SET ReposTickCount = ReposTickCount + 1 WHERE LocalReposID = {0}", localRepositoryId);
                command.ExecuteNonQuery();
                command.CommandText = String.Format("SELECT ReposTickCount FROM SyncRepositories WHERE LocalReposID = {0}", localRepositoryId);
                object o = command.ExecuteScalar();
                return (int)o;
            }
            finally
            {
                command.CommandText = "RELEASE UpdateTick";
                command.ExecuteNonQuery();
            }
        }


        #endregion

        #region Database Initialization

        private void initializeDatabase(IDbConnection connection)
        {
            using (IDbTransaction trans = connection.BeginTransaction())
            {
                try
                {
                    IDbCommand command = connection.CreateCommand();
                    command.Transaction = trans;


                    command.CommandText = "CREATE TABLE DBInfo(InfoID INTEGER PRIMARY KEY AUTOINCREMENT, InfoName TEXT, InfoStringValue TEXT, InfoIntegerValue INTEGER);";
                    command.ExecuteNonQuery();

                    command.CommandText = String.Format("INSERT INTO DBInfo VALUES(1,'Schema', '', {0} )", 1);
                    command.ExecuteNonQuery();

                    command.CommandText = "CREATE TABLE SyncRepositories(LocalReposID INTEGER PRIMARY KEY AUTOINCREMENT, GlobalReposID TEXT, ReposDesc TEXT, ReposName TEXT, ReposPassword TEXT, ReposTickCount INTEGER, ReposFailedLoginCount INTEGER);";
                    command.ExecuteNonQuery();

                    loadDefaultRepositories(trans);

                    command.CommandText = "CREATE TABLE Tombstones(TombstoneID INTEGER PRIMARY KEY AUTOINCREMENT, ItemType INTEGER, CreatedRepos INTEGER, CreatedTickCount INTEGER, ModifiedRepos INTEGER, ModifiedTickCount INTEGER, DeletionDateTime DATETIME, FOREIGN KEY(CreatedRepos) REFERENCES SyncRepositories(LocalReposID), FOREIGN KEY(ModifiedRepos) REFERENCES SyncRepositories(LocalReposID) );";
                    command.ExecuteNonQuery();

                    command.CommandText = String.Format("CREATE TABLE People(PersonID INTEGER PRIMARY KEY AUTOINCREMENT, PersonName TEXT, {0});", itemIdFieldsAndForiegnKeysForCreate);
                    command.ExecuteNonQuery();

                    command.CommandText = String.Format("CREATE TABLE Books(BookID INTEGER PRIMARY KEY AUTOINCREMENT, BookTitle TEXT, {0});", itemIdFieldsAndForiegnKeysForCreate);
                    command.ExecuteNonQuery();

                    command.CommandText = "CREATE TABLE BookAuthors(BookID INTEGER, PersonID INTEGER, AuthorPriority INTEGER, " +
                                                        "FOREIGN KEY(BookID) REFERENCES Books(BookID), " +
                                                        "FOREIGN KEY(PersonID) REFERENCES People(PersonID) " +
                                                        " );";
                    command.ExecuteNonQuery();

                    trans.Commit();
                }
                catch (Exception)
                {
                    trans.Rollback();
                    throw;
                }
            }
        }

        private void loadDefaultRepositories(IDbTransaction trans)
        {
            IDbCommand command = trans.Connection.CreateCommand();
            command.Transaction = trans;

            Guid.NewGuid().ToString();
            command.CommandText = String.Format("INSERT INTO SyncRepositories(LocalReposID,GlobalReposID,ReposTickCount,ReposDesc) VALUES(0,'{0}',{1},'')", Guid.NewGuid().ToString(), 0);
            command.ExecuteNonQuery();

            /* default values belong to the default repository */
            command.CommandText = String.Format("INSERT INTO SyncRepositories(LocalReposID,GlobalReposID,ReposTickCount,ReposDesc) VALUES(1,'DEFAULT_REPOS',{0},'Program Default Repository')", 0);
            command.ExecuteNonQuery();
        }

        #endregion

        internal IPerson getPersonByRowID(long rowId)
        {
            return (from item in AllPeople where ((Person)item).Id.RowId == rowId select item).First();
        }

        internal IBook getBookByRowID(long rowId)
        {
            return (from item in AllBooks where ((Book)item).Id.RowId == rowId select item).First();
        }

        private void loadAllPeople(IDbConnection connection)
        {
            if (_allPeople == null)
            {
                _allPeople = new ObservableCollection<IPerson>();
                _allPeopleReadOnly = new ReadOnlyObservableCollection<IPerson>(_allPeople);

                IDbCommand select = connection.CreateCommand();
                select.CommandText = String.Format("SELECT {0}, PersonName FROM People", itemIdFieldsForSelect);
                using (IDataReader reader = select.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        ReposItemId id = getReposItemIdFromDataReader(reader);
                        string name = Convert.ToString(reader["PersonName"]);
                        Person item = new Person(this, id, name);
                        _allPeople.Add(item);
                    }
                }
            }
        }

        private void loadAllBooks(IDbConnection connection)
        {
            if (_allBooks == null)
            {
                _allBooks = new ObservableCollection<IBook>();
                _allBooksReadOnly = new ReadOnlyObservableCollection<IBook>(_allBooks);

                IDbCommand select = connection.CreateCommand();
                select.CommandText = String.Format("SELECT {0}, BookTitle FROM Books", itemIdFieldsForSelect);
                using (IDataReader reader = select.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        ReposItemId id = getReposItemIdFromDataReader(reader);
                        string title = Convert.ToString(reader["BookTitle"]);
                        Book item = new Book(this, id, title);
                        _allBooks.Add(item);
                    }
                }
            }
        }

        internal ObservableCollection<IPerson> loadBookAuthors(long bookId)
        {
            ObservableCollection<IPerson> bookAuthors = new ObservableCollection<IPerson>();
            using (IDbConnection connection = createConnection())
            {
                IDbCommand select = connection.CreateCommand();
                select.CommandText = String.Format("SELECT PersonID FROM BookAuthors WHERE BookID={0} ORDER BY AuthorPriority", bookId);
                using (IDataReader reader = select.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        long personId = Convert.ToInt64(reader["PersonID"]);
                        IPerson person = getPersonByRowID(personId);
                        bookAuthors.Add(person);
                    }
                }
                return bookAuthors;
            }
        }


        internal void updateBook(IDbConnection connection, long bookId)
        {
            IDbCommand select = connection.CreateCommand();
            select.CommandText = String.Format("SELECT {0}, BookTitle FROM Books WHERE BookID={1}", itemIdFieldsForSelect, bookId);
            using (IDataReader reader = select.ExecuteReader())
            {
                while (reader.Read())
                {
                    ReposItemId id = getReposItemIdFromDataReader(reader);
                    string title = Convert.ToString(reader["BookTitle"]);
                    Book updatedBook = new Book(this, id, title);


                    bool found = false;
                    foreach (IBook iItem in _allBooks)
                    {
                        Book listItem = (Book)iItem;
                        if (listItem.Id.RowId == updatedBook.Id.RowId)
                        {
                            found = true;
                            listItem.copyItem(updatedBook);
                        }
                    }
                    if (!found)
                    {
                        _allBooks.Add(updatedBook);
                    }

                }
            }
        }

        internal void removeBook(long createdLocalRepositoryId, long createdTickCount)
        {
            foreach (IBook iItem in _allBooks)
            {
                Book listItem = (Book)iItem;
                if (listItem.Id.CreationRepositoryLocalId == createdLocalRepositoryId && listItem.Id.CreationTickCount == createdTickCount)
                {
                    _allBooks.Remove(listItem);
                    return;
                }
            }
        }

        internal void updatePerson(IDbConnection connection, long personId)
        {
            IDbCommand select = connection.CreateCommand();
            select.CommandText = String.Format("SELECT {0}, PersonName FROM People WHERE PersonID={1}", itemIdFieldsForSelect, personId);
            using (IDataReader reader = select.ExecuteReader())
            {
                while (reader.Read())
                {
                    ReposItemId id = getReposItemIdFromDataReader(reader);
                    string name = Convert.ToString(reader["PersonName"]);
                    Person updatedPerson = new Person(this, id, name);

                    bool found = false;
                    foreach (IPerson iItem in _allPeople)
                    {
                        Person listItem = (Person)iItem;
                        if (listItem.Id.RowId == updatedPerson.Id.RowId)
                        {
                            found = true;
                            listItem.copyItem(updatedPerson);
                        }
                    }
                    if (!found)
                    {
                        _allPeople.Add(updatedPerson);
                    }

                }
            }
        }

        internal void removePerson(long createdLocalRepositoryId, long createdTickCount)
        {
            foreach (IPerson iItem in _allPeople)
            {
                Person listItem = (Person)iItem;
                if (listItem.Id.CreationRepositoryLocalId == createdLocalRepositoryId && listItem.Id.CreationTickCount == createdTickCount)
                {
                    _allPeople.Remove(listItem);
                    return;
                }
            }
        }

        public IBook GetWriteableBook(IBook book)
        {
            Book item = (Book)book;
            return new Book(this, item);
        }

        public void DeleteBook(IBook book)
        {
            Book item = (Book)book;
            if (item.Id.RowId == 0)
                return;

            using (IDbConnection connection = createConnection())
            {
                string savepointName = "DeleteBook";

                IDbCommand command = connection.CreateCommand();
                command.CommandText = "SAVEPOINT " + savepointName;
                command.ExecuteNonQuery();
                try
                {
                    command.CommandText = "DELETE FROM BookAuthors WHERE BookID = @ID;";
                    command.AddParameter("@ID", item.Id.RowId);
                    command.ExecuteNonQuery();
                    command.Parameters.Clear();

                    command.CommandText = "DELETE FROM Books WHERE BookID = @ID;";
                    command.AddParameter("@ID", item.Id.RowId);
                    command.ExecuteNonQuery();
                    command.Parameters.Clear();

                    insertTombstone(connection, item.Id);

                    if (_allBooks.Contains(item))
                        _allBooks.Remove(item);

                    command.CommandText = "RELEASE " + savepointName;
                    command.ExecuteNonQuery();
                }
                catch (Exception)
                {
                    command.CommandText = "ROLLBACK TO SAVEPOINT " + savepointName;
                    command.ExecuteNonQuery();
                    throw;
                }
            }
        }

        public void SaveBook(IBook book)
        {
            Book bookToSave = (Book)book;
            if (!bookToSave.IsModified)
                return;

            using (IDbConnection connection = createConnection())
            {
                string savepointName = "SaveBook";

                IDbCommand command = connection.CreateCommand();
                command.CommandText = "SAVEPOINT " + savepointName;
                command.ExecuteNonQuery();
                try
                {
                    long tick = incrementTickCount(connection, 0);
                    if (bookToSave.Id.RowId == 0)
                    {
                        // Insert
                        command = connection.CreateCommand();
                        command.CommandText = "INSERT INTO Books(BookTitle, CreatedRepos, CreatedTickCount, ModifiedRepos, ModifiedTickCount) VALUES (@BookTitle, 0, @TickCount, 0, @TickCount)";
                        command.AddParameter("@BookTitle", bookToSave.Title);
                        command.AddParameter("@TickCount", tick);
                        command.ExecuteNonQuery();
                        bookToSave.Id.RowId = ((SqliteCommand)command).LastInsertRowID();
                        bookToSave.Id.CreationTickCount = tick;
                        bookToSave.Id.CreationRepositoryLocalId = 0;
                        command.Parameters.Clear();
                    }
                    else
                    {
                        // Update
                        command = connection.CreateCommand();
                        command.CommandText = "UPDATE Books SET ModifiedRepos=0, ModifiedTickCount=@TickCount, BookTitle=@BookTitle WHERE BookID=@RowID";
                        command.AddParameter("@TickCount", tick);
                        command.AddParameter("@BookTitle", bookToSave.Title);
                        command.AddParameter("@RowID", bookToSave.Id.RowId);
                        command.ExecuteNonQuery();
                        command.Parameters.Clear();
                    }
                    bookToSave.Id.ModificationRepositoryLocalId = 0;
                    bookToSave.Id.ModificationTickCount = tick;
                    bookToSave.IsReadOnly = true;
                    bookToSave.IsModified = false;


                    if (bookToSave.authorsChanged)
                    {
                        // First we need to delete any dependent connections
                        command.CommandText = "DELETE FROM BookAuthors WHERE BookID = @ID;";
                        command.AddParameter("@ID", bookToSave.Id.RowId);
                        command.ExecuteNonQuery();
                        command.Parameters.Clear();

                        int authorPriority = 0;
                        foreach (IPerson person in bookToSave.Authors)
                        {
                            Person personToInsert = (Person)person;
                            authorPriority++;
                            command.CommandText = String.Format("INSERT INTO BookAuthors(BookID, PersonID, AuthorPriority) VALUES ({0},{1},{2})", bookToSave.Id.RowId, personToInsert.Id.RowId, authorPriority);
                            command.ExecuteNonQuery();
                        }
                    }

                    command.CommandText = "RELEASE " + savepointName;
                    command.ExecuteNonQuery();


                    bool found = false;
                    foreach (IBook iItem in _allBooks)
                    {
                        Book listItem = (Book)iItem;
                        if (listItem.Id.RowId == bookToSave.Id.RowId)
                        {
                            found = true;
                            listItem.copyItem(bookToSave);
                        }
                    }
                    if (!found)
                    {
                        _allBooks.Add(bookToSave);
                    }

                }
                catch (Exception)
                {
                    command.CommandText = "ROLLBACK TO SAVEPOINT " + savepointName;
                    command.ExecuteNonQuery();
                    throw;
                }
            }
        }

        public ReadOnlyObservableCollection<IBook> AllBooks 
        { 
            get
            {
                return _allBooksReadOnly;
            }
        }


        public IPerson GetWriteablePerson(IPerson person)
        {
            Person item = (Person)person;
            return new Person(this, item);
        }

        public void DeletePerson(IPerson person)
        {
            Person personToDelete = (Person)person;
            if (personToDelete.Id.RowId == 0)
                return;

            using (IDbConnection connection = createConnection())
            {
                string savepointName = "DeletePerson";

                IDbCommand command = connection.CreateCommand();
                command.CommandText = "SAVEPOINT " + savepointName;
                command.ExecuteNonQuery();
                try
                {
                    command.CommandText = "DELETE FROM People WHERE PersonID = @ID;";
                    command.AddParameter("@ID", personToDelete.Id.RowId);
                    command.ExecuteNonQuery();
                    command.Parameters.Clear();

                    insertTombstone(connection, personToDelete.Id);

                    Person personToRemove = null;
                    foreach (IPerson p in _allPeople)
                    {
                        Person testPerson = (Person)p;
                        if (testPerson.Id.RowId == personToDelete.Id.RowId)
                        {
                            personToRemove = testPerson;
                        }
                    }
                    if (personToRemove != null)
                        _allPeople.Remove(personToRemove); 

                    command.CommandText = "RELEASE " + savepointName;
                    command.ExecuteNonQuery();
                }
                catch (Exception)
                {
                    command.CommandText = "ROLLBACK TO SAVEPOINT " + savepointName;
                    command.ExecuteNonQuery();
                    throw;
                }
            }
        }

        public void SavePerson(IPerson person)
        {
            Person personToSave = (Person)person;
            if (!personToSave.IsModified)
                return;

            using (IDbConnection connection = createConnection())
            {
                string savepointName = "SavePerson";

                IDbCommand command = connection.CreateCommand();
                command.CommandText = "SAVEPOINT " + savepointName;
                command.ExecuteNonQuery();
                try
                {
                    long tick = incrementTickCount(connection, 0);
                    if (personToSave.Id.RowId == 0)
                    {
                        // Insert
                        command = connection.CreateCommand();
                        command.CommandText = "INSERT INTO People(PersonName, CreatedRepos, CreatedTickCount, ModifiedRepos, ModifiedTickCount) VALUES (@PersonName, 0, @TickCount, 0, @TickCount)";
                        command.AddParameter("@PersonName", personToSave.Name);
                        command.AddParameter("@TickCount", tick);
                        command.ExecuteNonQuery();
                        personToSave.Id.RowId = ((SqliteCommand)command).LastInsertRowID();
                        personToSave.Id.CreationTickCount = tick;
                        personToSave.Id.CreationRepositoryLocalId = 0;
                        command.Parameters.Clear();
                    }
                    else
                    {
                        // Update
                        command = connection.CreateCommand();
                        command.CommandText = "UPDATE People SET ModifiedRepos=0, ModifiedTickCount=@TickCount, PersonName=@PersonName WHERE PersonID=@RowID";
                        command.AddParameter("@TickCount", tick);
                        command.AddParameter("@PersonName", personToSave.Name);
                        command.AddParameter("@RowID", personToSave.Id.RowId);
                        command.ExecuteNonQuery();
                        command.Parameters.Clear();
                    }
                    personToSave.Id.ModificationRepositoryLocalId = 0;
                    personToSave.Id.ModificationTickCount = tick;
                    personToSave.IsReadOnly = true;
                    personToSave.IsModified = false;

                    command.CommandText = "RELEASE " + savepointName;
                    command.ExecuteNonQuery();


                    bool found = false;
                    foreach (IPerson iItem in _allPeople)
                    {
                        Person listItem = (Person)iItem;
                        if (listItem.Id.RowId == personToSave.Id.RowId)
                        {
                            found = true;
                            listItem.copyItem(personToSave);
                        }
                    }
                    if (!found)
                    {
                        _allPeople.Add(personToSave);
                    }

                }
                catch (Exception)
                {
                    command.CommandText = "ROLLBACK TO SAVEPOINT " + savepointName;
                    command.ExecuteNonQuery();
                    throw;
                }
            }
        }

        public ReadOnlyObservableCollection<IPerson> AllPeople
        {
            get
            {
                return _allPeopleReadOnly;
            }
        }
    }
}
