using BookSample.Data;
using BookSample.Data.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;

namespace BookSample.WpfApplication
{
    /// <summary>
    /// Interaction logic for EditBookDialog.xaml
    /// </summary>
    public partial class EditBookDialog : Window
    {
        BookRepository _repos;
        public EditBookDialog(BookRepository repos, IBook book)
        {
            InitializeComponent();
            this._repos = repos;
            DataContext = book;
        }

        private void OkButton_Click(object sender, RoutedEventArgs e)
        {
            DialogResult = true;
            Close();
        }

        private void CancelButton_Click(object sender, RoutedEventArgs e)
        {
            DialogResult = false;
            Close();
        }

        private void peopleListBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            deleteAuthorButton.IsEnabled = peopleListBox.SelectedItem != null;
        }

        private void addAuthorButton_Click(object sender, RoutedEventArgs e)
        {
            SelectPersonDialog d = new SelectPersonDialog(_repos);
            d.Owner = this;
            bool? result = d.ShowDialog();
            if (result.HasValue && result.Value == true)
            {
                IBook book = DataContext as IBook;
                book.Authors.Add(d.SelectedPerson);
            }
        }

        private void deleteAuthorButton_Click(object sender, RoutedEventArgs e)
        {
            IBook book = DataContext as IBook;
            book.Authors.Remove(peopleListBox.SelectedItem as IPerson);
        }
    }
}
