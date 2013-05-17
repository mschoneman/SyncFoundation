using BookSample.Data;
using BookSample.Data.Interfaces;
using BookSample.Data.Sync;
using SyncFoundation.Client;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace BookSample.WpfApplication
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            InitializeComponent();
            this.synchronizationContext = SynchronizationContext.Current ?? new SynchronizationContext();
        }

        BookRepository _repos;
        SynchronizationContext synchronizationContext;

        private void OpenButton_Click(object sender, RoutedEventArgs e)
        {
            _repos = new BookRepository(pathTextBox.Text);
            bookListBox.ItemsSource = _repos.AllBooks;
            addButton.IsEnabled = true;
            editPeopleButton.IsEnabled = true;
            syncButton.IsEnabled = true;
        }

        private void addButton_Click(object sender, RoutedEventArgs e)
        {
            IBook writeableBook = _repos.GetWriteableBook(null);
            {
                EditBookDialog d = new EditBookDialog(_repos, writeableBook);
                d.Owner = this;
                bool? result = d.ShowDialog();
                if (result.HasValue && result.Value == true)
                {
                    _repos.SaveBook(writeableBook);
                }
            }
        }

        private void editButton_Click(object sender, RoutedEventArgs e)
        {
            IBook book = bookListBox.SelectedItem as IBook;
            IBook writeableBook = _repos.GetWriteableBook(book);
            {
                EditBookDialog d = new EditBookDialog(_repos, writeableBook);
                d.Owner = this;
                bool? result = d.ShowDialog();
                if (result.HasValue && result.Value == true)
                {
                    _repos.SaveBook(writeableBook);
                }
            }
        }

        private void deleteButton_Click(object sender, RoutedEventArgs e)
        {
            if (bookListBox.SelectedItem == null)
                return;
            IBook book = bookListBox.SelectedItem as IBook;
            _repos.DeleteBook(book);
        }

        private void bookListBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            editButton.IsEnabled = bookListBox.SelectedItem != null;
            deleteButton.IsEnabled = bookListBox.SelectedItem != null;
        }

        private void syncButton_Click(object sender, RoutedEventArgs e)
        {
            var session = new SyncSession(new BookRepositorySyncableStoreAdapter(_repos), new ClientSyncSessionDbConnectionProdivder(), new Uri("http://localhost:53831/"), "test@example.com", "monkey");
            var progressWatcher = new Progress<SyncProgress>(reportProgress);
            session.SyncWithRemoteAsync(progressWatcher, CancellationToken.None).Wait();
            session.Close();
        }

        private void reportProgress(SyncProgress obj)
        {
            this.synchronizationContext.Post((state) =>
            {
                SyncProgress progress = state as SyncProgress;
                syncProgressLabel.Content = progress.Message;
            },obj);
        }

        private void editPeopleButton_Click(object sender, RoutedEventArgs e)
        {
            EditPeopleDialog d = new EditPeopleDialog(_repos);
            d.Owner = this;
            d.ShowDialog();
        }

        private void stateButton_Click(object sender, RoutedEventArgs e)
        {
            MessageBox.Show(new BookRepositorySyncableStoreAdapter(_repos).GetDbState());
        }
    }
}
