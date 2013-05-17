using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using BookSample.Data.Sync;
using BookSample.Data;
using SyncFoundation.Client;
using System.Threading;
using BookSample.WpfApplication;
using BookSample.Data.Interfaces;
using System.Linq;
using System.Threading.Tasks;

namespace BookSample.Test
{
    [TestClass]
    public class BookSampleSyncTest
    {
        //private const string remoteHost = "http://localhost:53831/";
        private const string remoteHost = "http://localhost:18080/";
        private void resetCloud()
        {
            File.Delete(@"..\..\..\BookSample.WebService\App_Data\test@example.com.User.sqlite");
        }

        private BookRepositorySyncableStoreAdapter GetAdapter(string path)
        {
            File.Delete(path);
            var repos = new BookRepository(path);
            var adapter = new BookRepositorySyncableStoreAdapter(repos);
            return adapter;
        }

        [TestMethod]
        public void TestEmptyReplicaAreDifferent()
        {
            resetCloud();
            string file1 = "TEST1.sqlite";
            string file2 = "TEST2.sqlite";

            using (var adapter1 = GetAdapter(file1))
            using (var adapter2 = GetAdapter(file2))
            {
                var state1 = adapter1.GetDbState();
                var state2 = adapter2.GetDbState();
                Assert.AreNotEqual(state1, state2);
            }
        }

        private void syncAdatapersAssertNoConflicts(BookRepositorySyncableStoreAdapter adapter1, BookRepositorySyncableStoreAdapter adapter2)
        {
            // send any changes in 1 to cloud 
            var session1 = new SyncSession(adapter1, new ClientSyncSessionDbConnectionProdivder(), new Uri(remoteHost), "test@example.com", "monkey");
            var conflicts1 = session1.SyncWithRemoteAsync().Result;
            Assert.AreEqual(0, conflicts1.Count());
            session1.Close();

            // get changes from 1 and send any changes from 2
            var session2 = new SyncSession(adapter2, new ClientSyncSessionDbConnectionProdivder(), new Uri(remoteHost), "test@example.com", "monkey");
            var conflicts2 = session2.SyncWithRemoteAsync().Result;
            Assert.AreEqual(0, conflicts2.Count());
            session2.Close();

            // get any changes from 2 back into 1
            var session3 = new SyncSession(adapter1, new ClientSyncSessionDbConnectionProdivder(), new Uri(remoteHost), "test@example.com", "monkey");
            var conflicts3 = session3.SyncWithRemoteAsync().Result;
            Assert.AreEqual(0, conflicts3.Count());
            session3.Close();
        }

        [TestMethod]
        public void TestSyncEmptyReplica()
        {
            resetCloud();
            string file1 = "TEST1.sqlite";
            string file2 = "TEST2.sqlite";

            using (var adapter1 = GetAdapter(file1))
            using (var adapter2 = GetAdapter(file2))
            {
                var state1 = adapter1.GetDbState();
                var state2 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state3 = adapter1.GetDbState();
                var state4 = adapter2.GetDbState();
                Assert.AreNotEqual(state1, state2);
                Assert.AreEqual(state3, state4);
            }
        }

        [TestMethod]
        public void TestSyncPersonFrom1To2()
        {
            resetCloud();
            string file1 = "TEST1.sqlite";
            string file2 = "TEST2.sqlite";

            using (var adapter1 = GetAdapter(file1))
            using (var adapter2 = GetAdapter(file2))
            {
                addPerson(adapter1, "Test Person");

                var state1 = adapter1.GetDbState();
                var state2 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state3 = adapter1.GetDbState();
                var state4 = adapter2.GetDbState();

                Assert.AreEqual(1, adapter2.BookRepository.AllPeople.Count);
                Assert.AreEqual("Test Person", adapter2.BookRepository.AllPeople[0].Name);

                Assert.AreNotEqual(state1, state2);
                Assert.AreEqual(state3, state4);
            }
        }

        private static void addPerson(BookRepositorySyncableStoreAdapter adapter, string name)
        {
            IPerson person = adapter.BookRepository.GetWriteablePerson(null);
            person.Name = name;
            adapter.BookRepository.SavePerson(person);
        }

        [TestMethod]
        public void TestSyncPersonFrom2To1()
        {
            resetCloud();
            string file1 = "TEST1.sqlite";
            string file2 = "TEST2.sqlite";

            using (var adapter1 = GetAdapter(file1))
            using (var adapter2 = GetAdapter(file2))
            {
                addPerson(adapter2, "Test Person");

                var state1 = adapter1.GetDbState();
                var state2 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state3 = adapter1.GetDbState();
                var state4 = adapter2.GetDbState();

                Assert.AreEqual(1, adapter1.BookRepository.AllPeople.Count);
                Assert.AreEqual("Test Person", adapter1.BookRepository.AllPeople[0].Name);

                Assert.AreNotEqual(state1, state2);
                Assert.AreEqual(state3, state4);
            }
        }

        [TestMethod]
        public void TestSyncDuplicatePerson()
        {
            resetCloud();
            string file1 = "TEST1.sqlite";
            string file2 = "TEST2.sqlite";

            using (var adapter1 = GetAdapter(file1))
            using (var adapter2 = GetAdapter(file2))
            {
                addPerson(adapter1, "Test Person");
                addPerson(adapter2, "Test Person");

                var state1 = adapter1.GetDbState();
                var state2 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state3 = adapter1.GetDbState();
                var state4 = adapter2.GetDbState();

                Assert.AreEqual(1, adapter2.BookRepository.AllPeople.Count);
                Assert.AreEqual("Test Person", adapter2.BookRepository.AllPeople[0].Name);
                Assert.AreEqual(1, adapter1.BookRepository.AllPeople.Count);
                Assert.AreEqual("Test Person", adapter1.BookRepository.AllPeople[0].Name);

                Assert.AreNotEqual(state1, state2);
                Assert.AreEqual(state3, state4);
            }
        }

        [TestMethod]
        public void TestSyncPersonFrom1To2Delete1To2()
        {
            resetCloud();
            string file1 = "TEST1.sqlite";
            string file2 = "TEST2.sqlite";

            using (var adapter1 = GetAdapter(file1))
            using (var adapter2 = GetAdapter(file2))
            {
                addPerson(adapter1, "Test Person");

                var state1 = adapter1.GetDbState();
                var state2 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state3 = adapter1.GetDbState();
                var state4 = adapter2.GetDbState();

                Assert.AreEqual(1, adapter2.BookRepository.AllPeople.Count);
                Assert.AreEqual("Test Person", adapter2.BookRepository.AllPeople[0].Name);

                adapter1.BookRepository.DeletePerson(adapter1.BookRepository.AllPeople[0]);
                var state5 = adapter1.GetDbState();
                var state6 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state7 = adapter1.GetDbState();
                var state8 = adapter2.GetDbState();


                Assert.AreNotEqual(state1, state2);
                Assert.AreEqual(state3, state4);
                Assert.AreNotEqual(state5, state6);
                Assert.AreEqual(state5, state7);
                Assert.AreEqual(state7, state8);
            }
        }


        [TestMethod]
        public void TestSyncPersonFrom1To2Delete2To1()
        {
            resetCloud();
            string file1 = "TEST1.sqlite";
            string file2 = "TEST2.sqlite";

            using (var adapter1 = GetAdapter(file1))
            using (var adapter2 = GetAdapter(file2))
            {
                addPerson(adapter1, "Test Person");

                var state1 = adapter1.GetDbState();
                var state2 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state3 = adapter1.GetDbState();
                var state4 = adapter2.GetDbState();

                Assert.AreEqual(1, adapter2.BookRepository.AllPeople.Count);
                Assert.AreEqual("Test Person", adapter2.BookRepository.AllPeople[0].Name);

                adapter2.BookRepository.DeletePerson(adapter2.BookRepository.AllPeople[0]);
                var state5 = adapter1.GetDbState();
                var state6 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state7 = adapter1.GetDbState();
                var state8 = adapter2.GetDbState();


                Assert.AreNotEqual(state1, state2);
                Assert.AreEqual(state3, state4);
                Assert.AreNotEqual(state5, state6);
                Assert.AreEqual(state6, state8);
                Assert.AreEqual(state7, state8);
            }
        }

        [TestMethod]
        public void TestSyncBookFrom1To2()
        {
            resetCloud();
            string file1 = "TEST1.sqlite";
            string file2 = "TEST2.sqlite";

            using (var adapter1 = GetAdapter(file1))
            using (var adapter2 = GetAdapter(file2))
            {
                addBook(adapter1, "Test Book");

                var state1 = adapter1.GetDbState();
                var state2 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state3 = adapter1.GetDbState();
                var state4 = adapter2.GetDbState();

                Assert.AreEqual(1, adapter2.BookRepository.AllBooks.Count);
                Assert.AreEqual("Test Book", adapter2.BookRepository.AllBooks[0].Title);

                Assert.AreNotEqual(state1, state2);
                Assert.AreEqual(state3, state4);
            }
        }

        private static void addBook(BookRepositorySyncableStoreAdapter adapter, string title)
        {
            IBook Book = adapter.BookRepository.GetWriteableBook(null);
            Book.Title = title;
            adapter.BookRepository.SaveBook(Book);
        }

        [TestMethod]
        public void TestSyncBookFrom2To1()
        {
            resetCloud();
            string file1 = "TEST1.sqlite";
            string file2 = "TEST2.sqlite";

            using (var adapter1 = GetAdapter(file1))
            using (var adapter2 = GetAdapter(file2))
            {
                addBook(adapter2, "Test Book");

                var state1 = adapter1.GetDbState();
                var state2 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state3 = adapter1.GetDbState();
                var state4 = adapter2.GetDbState();

                Assert.AreEqual(1, adapter1.BookRepository.AllBooks.Count);
                Assert.AreEqual("Test Book", adapter1.BookRepository.AllBooks[0].Title);

                Assert.AreNotEqual(state1, state2);
                Assert.AreEqual(state3, state4);
            }
        }

        [TestMethod]
        public void TestSyncDuplicateBook()
        {
            resetCloud();
            string file1 = "TEST1.sqlite";
            string file2 = "TEST2.sqlite";

            using (var adapter1 = GetAdapter(file1))
            using (var adapter2 = GetAdapter(file2))
            {
                addBook(adapter1, "Test Book");
                addBook(adapter2, "Test Book");

                var state1 = adapter1.GetDbState();
                var state2 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state3 = adapter1.GetDbState();
                var state4 = adapter2.GetDbState();

                Assert.AreEqual(1, adapter2.BookRepository.AllBooks.Count);
                Assert.AreEqual("Test Book", adapter2.BookRepository.AllBooks[0].Title);
                Assert.AreEqual(1, adapter1.BookRepository.AllBooks.Count);
                Assert.AreEqual("Test Book", adapter1.BookRepository.AllBooks[0].Title);

                Assert.AreNotEqual(state1, state2);
                Assert.AreEqual(state3, state4);
            }
        }

        [TestMethod]
        public void TestSyncDupBookDupPeoplePlusNonDups()
        {
            resetCloud();
            string file1 = "TEST1.sqlite";
            string file2 = "TEST2.sqlite";

            using (var adapter1 = GetAdapter(file1))
            using (var adapter2 = GetAdapter(file2))
            {
                addPerson(adapter1, "Person 1");
                addPerson(adapter1, "Person 2");

                addPerson(adapter2, "Person 1");
                addPerson(adapter2, "Person 3");

                addBook(adapter1, "Book 1");
                addBook(adapter1, "Book 2");

                addBook(adapter2, "Book 1");
                addBook(adapter2, "Book 3");

                var state1 = adapter1.GetDbState();
                var state2 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state3 = adapter1.GetDbState();
                var state4 = adapter2.GetDbState();

                Assert.AreNotEqual(state1, state2);
                Assert.AreEqual(state3, state4);
            }
        }

        [TestMethod]
        public void TestSyncBookWithAuthorNoDups()
        {
            resetCloud();
            string file1 = "TEST1.sqlite";
            string file2 = "TEST2.sqlite";

            using (var adapter1 = GetAdapter(file1))
            using (var adapter2 = GetAdapter(file2))
            {
                addPerson(adapter1, "Person 1");
                addBook(adapter1, "Book 1");
                addPersonToBook(adapter1, "Book 1", "Person 1");
                
                var state1 = adapter1.GetDbState();
                var state2 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state3 = adapter1.GetDbState();
                var state4 = adapter2.GetDbState();

                Assert.AreEqual(1, adapter1.BookRepository.AllPeople.Count);
                Assert.AreEqual(1, adapter2.BookRepository.AllPeople.Count);

                Assert.AreEqual(1, adapter1.BookRepository.AllBooks.Count);
                Assert.AreEqual(1, adapter2.BookRepository.AllBooks.Count);

                Assert.AreNotEqual(state1, state2);
                Assert.AreEqual(state3, state4);
            }
        }

        private void addPersonToBook(BookRepositorySyncableStoreAdapter adapter1, string title, string name)
        {
            IPerson person = (from p in adapter1.BookRepository.AllPeople where p.Name == name select p).First();
            IBook book = (from p in adapter1.BookRepository.AllBooks where p.Title == title select p).First();
            IBook bookWritable = adapter1.BookRepository.GetWriteableBook(book);
            bookWritable.Authors.Add(person);
            adapter1.BookRepository.SaveBook(bookWritable);
        }


        [TestMethod]
        public void TestSyncBookWithAuthorDups()
        {
            resetCloud();
            string file1 = "TEST1.sqlite";
            string file2 = "TEST2.sqlite";

            using (var adapter1 = GetAdapter(file1))
            using (var adapter2 = GetAdapter(file2))
            {
                addPerson(adapter1, "Person 1");
                addBook(adapter1, "Book 1");
                addPersonToBook(adapter1, "Book 1", "Person 1");
                addPerson(adapter2, "Person 1");
                addBook(adapter2, "Book 1");
                addPersonToBook(adapter2, "Book 1", "Person 1");

                addPerson(adapter1, "Person 2");
                addBook(adapter1, "Book 2");
                addPersonToBook(adapter1, "Book 2", "Person 2");

                addPerson(adapter2, "Person 3");
                addBook(adapter2, "Book 3");
                addPersonToBook(adapter2, "Book 3", "Person 3");

                var state1 = adapter1.GetDbState();
                var state2 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state3 = adapter1.GetDbState();
                var state4 = adapter2.GetDbState();

                Assert.AreEqual(3, adapter1.BookRepository.AllPeople.Count);
                Assert.AreEqual(3, adapter2.BookRepository.AllPeople.Count);

                Assert.AreEqual(3, adapter1.BookRepository.AllBooks.Count);
                Assert.AreEqual(3, adapter2.BookRepository.AllBooks.Count);

                Assert.AreNotEqual(state1, state2);
                Assert.AreEqual(state3, state4);
            }
        }

        [TestMethod]
        public void TestSyncBookWithAuthorDups2()
        {
            resetCloud();
            string file1 = "TEST1.sqlite";
            string file2 = "TEST2.sqlite";

            using (var adapter1 = GetAdapter(file1))
            using (var adapter2 = GetAdapter(file2))
            {
                addPerson(adapter1, "Person 1");
                addBook(adapter1, "Book 1");
                addPersonToBook(adapter1, "Book 1", "Person 1");
                addPerson(adapter2, "Person 1");
                addBook(adapter2, "Book 1");
                addPersonToBook(adapter2, "Book 1", "Person 1");

                addPerson(adapter1, "Person 2");
                addBook(adapter1, "Book 2");
                addPersonToBook(adapter1, "Book 2", "Person 2");

                addPerson(adapter2, "Person 2");
                addPerson(adapter2, "Person 3");
                addBook(adapter2, "Book 3");
                addPersonToBook(adapter2, "Book 3", "Person 3");

                var state1 = adapter1.GetDbState();
                var state2 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state3 = adapter1.GetDbState();
                var state4 = adapter2.GetDbState();

                Assert.AreEqual(3, adapter1.BookRepository.AllPeople.Count);
                Assert.AreEqual(3, adapter2.BookRepository.AllPeople.Count);

                Assert.AreEqual(3, adapter1.BookRepository.AllBooks.Count);
                Assert.AreEqual(3, adapter2.BookRepository.AllBooks.Count);

                Assert.AreNotEqual(state1, state2);
                Assert.AreEqual(state3, state4);
            }
        }

        [TestMethod]
        public void TestSyncBookWithAuthorDups3()
        {
            resetCloud();
            string file1 = "TEST1.sqlite";
            string file2 = "TEST2.sqlite";

            using (var adapter1 = GetAdapter(file1))
            using (var adapter2 = GetAdapter(file2))
            {
                addPerson(adapter1, "Person 1");
                addBook(adapter1, "Book 1");
                addPersonToBook(adapter1, "Book 1", "Person 1");
                addPerson(adapter2, "Person 1");
                addBook(adapter2, "Book 1");
                addPersonToBook(adapter2, "Book 1", "Person 1");

                addPerson(adapter1, "Person 3");
                addPerson(adapter1, "Person 2");
                addBook(adapter1, "Book 2");
                addPersonToBook(adapter1, "Book 2", "Person 2");

                addPerson(adapter2, "Person 3");
                addBook(adapter2, "Book 3");
                addPersonToBook(adapter2, "Book 3", "Person 3");

                var state1 = adapter1.GetDbState();
                var state2 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state3 = adapter1.GetDbState();
                var state4 = adapter2.GetDbState();

                Assert.AreEqual(3, adapter1.BookRepository.AllPeople.Count);
                Assert.AreEqual(3, adapter2.BookRepository.AllPeople.Count);

                Assert.AreEqual(3, adapter1.BookRepository.AllBooks.Count);
                Assert.AreEqual(3, adapter2.BookRepository.AllBooks.Count);

                Assert.AreNotEqual(state1, state2);
                Assert.AreEqual(state3, state4);
            }
        }

        [TestMethod]
        public void TestSyncBookWithAuthorDups4()
        {
            resetCloud();
            string file1 = "TEST1.sqlite";
            string file2 = "TEST2.sqlite";

            using (var adapter1 = GetAdapter(file1))
            using (var adapter2 = GetAdapter(file2))
            {
                addPerson(adapter1, "Person 1");
                addBook(adapter1, "Book 1");
                addPersonToBook(adapter1, "Book 1", "Person 1");
                addPerson(adapter2, "Person 1");
                addBook(adapter2, "Book 1");
                addPersonToBook(adapter2, "Book 1", "Person 1");

                addPerson(adapter1, "Person 3");
                addBook(adapter1, "Book 4");
                addPersonToBook(adapter1, "Book 4", "Person 3");
                addPerson(adapter1, "Person 2");
                addBook(adapter1, "Book 2");
                addPersonToBook(adapter1, "Book 2", "Person 2");

                addPerson(adapter2, "Person 3");
                addBook(adapter2, "Book 3");
                addPersonToBook(adapter2, "Book 3", "Person 3");

                var state1 = adapter1.GetDbState();
                var state2 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state3 = adapter1.GetDbState();
                var state4 = adapter2.GetDbState();

                Assert.AreEqual(3, adapter1.BookRepository.AllPeople.Count);
                Assert.AreEqual(3, adapter2.BookRepository.AllPeople.Count);

                Assert.AreEqual(4, adapter1.BookRepository.AllBooks.Count);
                Assert.AreEqual(4, adapter2.BookRepository.AllBooks.Count);

                Assert.AreNotEqual(state1, state2);
                Assert.AreEqual(state3, state4);
            }
        }


        [TestMethod]
        public void TestSyncBookWithAuthorDups5()
        {
            resetCloud();
            string file1 = "TEST1.sqlite";
            string file2 = "TEST2.sqlite";

            using (var adapter1 = GetAdapter(file1))
            using (var adapter2 = GetAdapter(file2))
            {
                addPerson(adapter1, "Person 1");
                addBook(adapter1, "Book 1");
                addPersonToBook(adapter1, "Book 1", "Person 1");
                addPerson(adapter2, "Person 1");
                addBook(adapter2, "Book 1");
                addPersonToBook(adapter2, "Book 1", "Person 1");

                addPerson(adapter1, "Person 3");
                addBook(adapter1, "Book 4");
                addPersonToBook(adapter1, "Book 4", "Person 3");
                addPerson(adapter1, "Person 2");
                addBook(adapter1, "Book 2");
                addPersonToBook(adapter1, "Book 2", "Person 2");

                addPerson(adapter2, "Person 3");
                addBook(adapter2, "Book 3");
                addPersonToBook(adapter2, "Book 3", "Person 3");
                addPerson(adapter2, "Person 2");
                addBook(adapter2, "Book 5");
                addPersonToBook(adapter2, "Book 5", "Person 2");

                var state1 = adapter1.GetDbState();
                var state2 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state3 = adapter1.GetDbState();
                var state4 = adapter2.GetDbState();

                Assert.AreEqual(3, adapter1.BookRepository.AllPeople.Count);
                Assert.AreEqual(3, adapter2.BookRepository.AllPeople.Count);

                Assert.AreEqual(5, adapter1.BookRepository.AllBooks.Count);
                Assert.AreEqual(5, adapter2.BookRepository.AllBooks.Count);

                Assert.AreNotEqual(state1, state2);
                Assert.AreEqual(state3, state4);
            }
        }


        [TestMethod]
        public void TestSyncBookModification()
        {
            resetCloud();
            string file1 = "TEST1.sqlite";
            string file2 = "TEST2.sqlite";

            using (var adapter1 = GetAdapter(file1))
            using (var adapter2 = GetAdapter(file2))
            {
                addBook(adapter1, "Book 1");

                var state1 = adapter1.GetDbState();
                var state2 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state3 = adapter1.GetDbState();
                var state4 = adapter2.GetDbState();

                IBook writeableBook = adapter1.BookRepository.GetWriteableBook(adapter1.BookRepository.AllBooks[0]);
                writeableBook.Title = "Modified 1";
                adapter1.BookRepository.SaveBook(writeableBook);

                var state5 = adapter1.GetDbState();
                var state6 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state7 = adapter1.GetDbState();
                var state8 = adapter2.GetDbState();

                Assert.AreEqual(1, adapter1.BookRepository.AllBooks.Count);
                Assert.AreEqual(1, adapter2.BookRepository.AllBooks.Count);
                Assert.AreEqual("Modified 1", adapter1.BookRepository.AllBooks[0].Title);
                Assert.AreEqual("Modified 1", adapter2.BookRepository.AllBooks[0].Title);

                Assert.AreNotEqual(state1, state2);
                Assert.AreEqual(state3, state4);
                Assert.AreNotEqual(state5, state6);
                Assert.AreEqual(state5, state7);
                Assert.AreEqual(state7, state8);
            }
        }

        [TestMethod]
        public void TestSyncBookAddAuthorModification()
        {
            resetCloud();
            string file1 = "TEST1.sqlite";
            string file2 = "TEST2.sqlite";

            using (var adapter1 = GetAdapter(file1))
            using (var adapter2 = GetAdapter(file2))
            {
                addBook(adapter1, "Book 1");

                var state1 = adapter1.GetDbState();
                var state2 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state3 = adapter1.GetDbState();
                var state4 = adapter2.GetDbState();

                addPerson(adapter1, "Person 1");
                addPersonToBook(adapter1, "Book 1", "Person 1");

                var state5 = adapter1.GetDbState();
                var state6 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state7 = adapter1.GetDbState();
                var state8 = adapter2.GetDbState();

                Assert.AreEqual(1, adapter1.BookRepository.AllPeople.Count);
                Assert.AreEqual(1, adapter2.BookRepository.AllPeople.Count);
                Assert.AreEqual(1, adapter1.BookRepository.AllBooks.Count);
                Assert.AreEqual(1, adapter2.BookRepository.AllBooks.Count);
                Assert.AreEqual(1, adapter1.BookRepository.AllBooks[0].Authors.Count);
                Assert.AreEqual(1, adapter2.BookRepository.AllBooks[0].Authors.Count);
                Assert.AreEqual("Book 1", adapter1.BookRepository.AllBooks[0].Title);
                Assert.AreEqual("Book 1", adapter2.BookRepository.AllBooks[0].Title);
                Assert.AreEqual("Person 1", adapter1.BookRepository.AllBooks[0].Authors[0].Name);
                Assert.AreEqual("Person 1", adapter2.BookRepository.AllBooks[0].Authors[0].Name);

                Assert.AreNotEqual(state1, state2);
                Assert.AreEqual(state3, state4);
                Assert.AreNotEqual(state5, state6);
                Assert.AreEqual(state5, state7);
                Assert.AreEqual(state7, state8);
            }
        }

        [TestMethod]
        public void TestSyncBookAddDupAuthorModification()
        {
            resetCloud();
            string file1 = "TEST1.sqlite";
            string file2 = "TEST2.sqlite";

            using (var adapter1 = GetAdapter(file1))
            using (var adapter2 = GetAdapter(file2))
            {
                addBook(adapter1, "Book 1");

                var state1 = adapter1.GetDbState();
                var state2 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state3 = adapter1.GetDbState();
                var state4 = adapter2.GetDbState();

                addPerson(adapter1, "Person 1");
                addPersonToBook(adapter1, "Book 1", "Person 1");
                addPerson(adapter2, "Person 1");
                addPersonToBook(adapter2, "Book 1", "Person 1");

                var state5 = adapter1.GetDbState();
                var state6 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state7 = adapter1.GetDbState();
                var state8 = adapter2.GetDbState();

                Assert.AreEqual(1, adapter1.BookRepository.AllPeople.Count);
                Assert.AreEqual(1, adapter2.BookRepository.AllPeople.Count);
                Assert.AreEqual(1, adapter1.BookRepository.AllBooks.Count);
                Assert.AreEqual(1, adapter2.BookRepository.AllBooks.Count);
                Assert.AreEqual(1, adapter1.BookRepository.AllBooks[0].Authors.Count);
                Assert.AreEqual(1, adapter2.BookRepository.AllBooks[0].Authors.Count);
                Assert.AreEqual("Book 1", adapter1.BookRepository.AllBooks[0].Title);
                Assert.AreEqual("Book 1", adapter2.BookRepository.AllBooks[0].Title);
                Assert.AreEqual("Person 1", adapter1.BookRepository.AllBooks[0].Authors[0].Name);
                Assert.AreEqual("Person 1", adapter2.BookRepository.AllBooks[0].Authors[0].Name);

                Assert.AreNotEqual(state1, state2);
                Assert.AreEqual(state3, state4);
                Assert.AreNotEqual(state5, state6);
                Assert.AreEqual(state7, state8);
            }
        }

        [TestMethod]
        public void TestSyncBookRemoveAuthorModification()
        {
            resetCloud();
            string file1 = "TEST1.sqlite";
            string file2 = "TEST2.sqlite";

            using (var adapter1 = GetAdapter(file1))
            using (var adapter2 = GetAdapter(file2))
            {
                addBook(adapter1, "Book 1");
                addPerson(adapter1, "Person 1");
                addPersonToBook(adapter1, "Book 1", "Person 1");

                var state1 = adapter1.GetDbState();
                var state2 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state3 = adapter1.GetDbState();
                var state4 = adapter2.GetDbState();

                IBook writeableBook = adapter1.BookRepository.GetWriteableBook(adapter1.BookRepository.AllBooks[0]);
                writeableBook.Authors.Clear();
                adapter1.BookRepository.SaveBook(writeableBook);


                var state5 = adapter1.GetDbState();
                var state6 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state7 = adapter1.GetDbState();
                var state8 = adapter2.GetDbState();

                Assert.AreEqual(1, adapter1.BookRepository.AllPeople.Count);
                Assert.AreEqual(1, adapter2.BookRepository.AllPeople.Count);
                Assert.AreEqual(1, adapter1.BookRepository.AllBooks.Count);
                Assert.AreEqual(1, adapter2.BookRepository.AllBooks.Count);
                Assert.AreEqual(0, adapter1.BookRepository.AllBooks[0].Authors.Count);
                Assert.AreEqual(0, adapter2.BookRepository.AllBooks[0].Authors.Count);

                Assert.AreNotEqual(state1, state2);
                Assert.AreEqual(state3, state4);
                Assert.AreNotEqual(state5, state6);
                Assert.AreEqual(state5, state7);
                Assert.AreEqual(state7, state8);
            }
        }

        [TestMethod]
        public void TestSyncBookRemoveAndDeleteAuthorModification()
        {
            resetCloud();
            string file1 = "TEST1.sqlite";
            string file2 = "TEST2.sqlite";

            using (var adapter1 = GetAdapter(file1))
            using (var adapter2 = GetAdapter(file2))
            {
                addBook(adapter1, "Book 1");
                addPerson(adapter1, "Person 1");
                addPersonToBook(adapter1, "Book 1", "Person 1");

                var state1 = adapter1.GetDbState();
                var state2 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state3 = adapter1.GetDbState();
                var state4 = adapter2.GetDbState();

                IBook writeableBook = adapter1.BookRepository.GetWriteableBook(adapter1.BookRepository.AllBooks[0]);
                writeableBook.Authors.Clear();
                adapter1.BookRepository.SaveBook(writeableBook);
                adapter1.BookRepository.DeletePerson(adapter1.BookRepository.AllPeople[0]);

                var state5 = adapter1.GetDbState();
                var state6 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state7 = adapter1.GetDbState();
                var state8 = adapter2.GetDbState();

                Assert.AreEqual(0, adapter1.BookRepository.AllPeople.Count);
                Assert.AreEqual(0, adapter2.BookRepository.AllPeople.Count);
                Assert.AreEqual(1, adapter1.BookRepository.AllBooks.Count);
                Assert.AreEqual(1, adapter2.BookRepository.AllBooks.Count);
                Assert.AreEqual(0, adapter1.BookRepository.AllBooks[0].Authors.Count);
                Assert.AreEqual(0, adapter2.BookRepository.AllBooks[0].Authors.Count);

                Assert.AreNotEqual(state1, state2);
                Assert.AreEqual(state3, state4);
                Assert.AreNotEqual(state5, state6);
                Assert.AreEqual(state5, state7);
                Assert.AreEqual(state7, state8);
            }
        }


        [TestMethod]
        public void TestSyncPersonModification()
        {
            resetCloud();
            string file1 = "TEST1.sqlite";
            string file2 = "TEST2.sqlite";

            using (var adapter1 = GetAdapter(file1))
            using (var adapter2 = GetAdapter(file2))
            {
                addPerson(adapter1, "Person 1");

                var state1 = adapter1.GetDbState();
                var state2 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state3 = adapter1.GetDbState();
                var state4 = adapter2.GetDbState();

                IPerson writeablePerson = adapter1.BookRepository.GetWriteablePerson(adapter1.BookRepository.AllPeople[0]);
                writeablePerson.Name = "Modified 1";
                adapter1.BookRepository.SavePerson(writeablePerson);

                var state5 = adapter1.GetDbState();
                var state6 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state7 = adapter1.GetDbState();
                var state8 = adapter2.GetDbState();

                Assert.AreEqual(1, adapter1.BookRepository.AllPeople.Count);
                Assert.AreEqual(1, adapter2.BookRepository.AllPeople.Count);
                Assert.AreEqual("Modified 1", adapter1.BookRepository.AllPeople[0].Name);
                Assert.AreEqual("Modified 1", adapter2.BookRepository.AllPeople[0].Name);

                Assert.AreNotEqual(state1, state2);
                Assert.AreEqual(state3, state4);
                Assert.AreNotEqual(state5, state6);
                Assert.AreEqual(state5, state7);
                Assert.AreEqual(state7, state8);
            }
        }

        [TestMethod]
        public async Task TestSyncPersonConflictResolveLocal()
        {
            resetCloud();
            string file1 = "TEST1.sqlite";
            string file2 = "TEST2.sqlite";

            using (var adapter1 = GetAdapter(file1))
            using (var adapter2 = GetAdapter(file2))
            {
                addPerson(adapter1, "Person 1");

                var state1 = adapter1.GetDbState();
                var state2 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state3 = adapter1.GetDbState();
                var state4 = adapter2.GetDbState();

                IPerson writeablePerson = adapter1.BookRepository.GetWriteablePerson(adapter1.BookRepository.AllPeople[0]);
                writeablePerson.Name = "Modified 1";
                adapter1.BookRepository.SavePerson(writeablePerson);

                writeablePerson = adapter2.BookRepository.GetWriteablePerson(adapter2.BookRepository.AllPeople[0]);
                writeablePerson.Name = "Modified 2";
                adapter2.BookRepository.SavePerson(writeablePerson);


                // send any changes in 1 to cloud 
                var session1 = new SyncSession(adapter1, new ClientSyncSessionDbConnectionProdivder(), new Uri(remoteHost), "test@example.com", "monkey");
                session1.SyncWithRemoteAsync().Wait();
                var conflicts1 = await session1.SyncWithRemoteAsync();
                Assert.AreEqual(0, conflicts1.Count());
                if (conflicts1.Any())
                {
                    foreach (var conflict in conflicts1)
                        session1.ResolveConflictLocalWins(conflict);
                    conflicts1 = await session1.SyncWithRemoteAsync();
                }
                session1.Close();

                // get changes from 1 and send any changes from 2
                var session2 = new SyncSession(adapter2, new ClientSyncSessionDbConnectionProdivder(), new Uri(remoteHost), "test@example.com", "monkey");
                var conflicts2 = await session2.SyncWithRemoteAsync();
                Assert.AreEqual(1, conflicts2.Count());
                if (conflicts2.Any())
                {
                    foreach (var conflict in conflicts2)
                        session2.ResolveConflictLocalWins(conflict);
                    conflicts2 = await session2.SyncWithRemoteAsync();
                }
                session2.Close();

                // get any changes from 2 back into 1
                var session3 = new SyncSession(adapter1, new ClientSyncSessionDbConnectionProdivder(), new Uri(remoteHost), "test@example.com", "monkey");
                var conflicts3 = await session3.SyncWithRemoteAsync();
                Assert.AreEqual(0, conflicts3.Count());
                if (conflicts2.Any())
                {
                    foreach (var conflict in conflicts3)
                        session3.ResolveConflictLocalWins(conflict);
                    conflicts3 = await session3.SyncWithRemoteAsync();
                }
                session3.Close();


                var state7 = adapter1.GetDbState();
                var state8 = adapter2.GetDbState();

                Assert.AreEqual(0, conflicts1.Count());
                Assert.AreEqual(0, conflicts2.Count());
                Assert.AreEqual(0, conflicts3.Count());

                Assert.AreEqual(1, adapter1.BookRepository.AllPeople.Count);
                Assert.AreEqual(1, adapter2.BookRepository.AllPeople.Count);
                Assert.AreEqual("Modified 2", adapter1.BookRepository.AllPeople[0].Name);
                Assert.AreEqual("Modified 2", adapter2.BookRepository.AllPeople[0].Name);

                Assert.AreNotEqual(state1, state2);
                Assert.AreEqual(state3, state4);
                Assert.AreEqual(state7, state8);
            }
        }

        [TestMethod]
        public async Task TestSyncPersonConflictResolveRemote()
        {
            resetCloud();
            string file1 = "TEST1.sqlite";
            string file2 = "TEST2.sqlite";

            using (var adapter1 = GetAdapter(file1))
            using (var adapter2 = GetAdapter(file2))
            {
                addPerson(adapter1, "Person 1");

                var state1 = adapter1.GetDbState();
                var state2 = adapter2.GetDbState();
                syncAdatapersAssertNoConflicts(adapter1, adapter2);
                var state3 = adapter1.GetDbState();
                var state4 = adapter2.GetDbState();

                IPerson writeablePerson = adapter1.BookRepository.GetWriteablePerson(adapter1.BookRepository.AllPeople[0]);
                writeablePerson.Name = "Modified 1";
                adapter1.BookRepository.SavePerson(writeablePerson);

                writeablePerson = adapter2.BookRepository.GetWriteablePerson(adapter2.BookRepository.AllPeople[0]);
                writeablePerson.Name = "Modified 2";
                adapter2.BookRepository.SavePerson(writeablePerson);


                // send any changes in 1 to cloud 
                var session1 = new SyncSession(adapter1, new ClientSyncSessionDbConnectionProdivder(), new Uri(remoteHost), "test@example.com", "monkey");
                session1.SyncWithRemoteAsync().Wait();
                var conflicts1 = await session1.SyncWithRemoteAsync();
                Assert.AreEqual(0, conflicts1.Count());
                if (conflicts1.Any())
                {
                    foreach (var conflict in conflicts1)
                        session1.ResolveConflictRemoteWins(conflict);
                    conflicts1 = await session1.SyncWithRemoteAsync();
                }
                session1.Close();

                // get changes from 1 and send any changes from 2
                var session2 = new SyncSession(adapter2, new ClientSyncSessionDbConnectionProdivder(), new Uri(remoteHost), "test@example.com", "monkey");
                var conflicts2 = await session2.SyncWithRemoteAsync();
                Assert.AreEqual(1, conflicts2.Count());
                if (conflicts2.Any())
                {
                    foreach (var conflict in conflicts2)
                        session2.ResolveConflictRemoteWins(conflict);
                    conflicts2 = await session2.SyncWithRemoteAsync();
                }
                session2.Close();

                // get any changes from 2 back into 1
                var session3 = new SyncSession(adapter1, new ClientSyncSessionDbConnectionProdivder(), new Uri(remoteHost), "test@example.com", "monkey");
                var conflicts3 = await session3.SyncWithRemoteAsync();
                Assert.AreEqual(0, conflicts3.Count());
                if (conflicts2.Any())
                {
                    foreach (var conflict in conflicts3)
                        session3.ResolveConflictRemoteWins(conflict);
                    conflicts3 = await session3.SyncWithRemoteAsync();
                }
                session3.Close();


                var state7 = adapter1.GetDbState();
                var state8 = adapter2.GetDbState();

                Assert.AreEqual(0, conflicts1.Count());
                Assert.AreEqual(0, conflicts2.Count());
                Assert.AreEqual(0, conflicts3.Count());

                Assert.AreEqual(1, adapter1.BookRepository.AllPeople.Count);
                Assert.AreEqual(1, adapter2.BookRepository.AllPeople.Count);
                Assert.AreEqual("Modified 1", adapter1.BookRepository.AllPeople[0].Name);
                Assert.AreEqual("Modified 1", adapter2.BookRepository.AllPeople[0].Name);

                Assert.AreNotEqual(state1, state2);
                Assert.AreEqual(state3, state4);
                Assert.AreEqual(state7, state8);
            }
        }

    }
}
