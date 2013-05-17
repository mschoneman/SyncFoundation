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
    /// Interaction logic for EditPeopleDialog.xaml
    /// </summary>
    public partial class EditPeopleDialog : Window
    {
        BookRepository _repos;

        public EditPeopleDialog(BookRepository repos)
        {
            InitializeComponent();
            this._repos = repos;
            peopleListBox.ItemsSource = _repos.AllPeople;
        }

        private void addButton_Click(object sender, RoutedEventArgs e)
        {
            IPerson writeablePerson = _repos.GetWriteablePerson(null);
            {
                EditPersonDialog d = new EditPersonDialog(writeablePerson);
                d.Owner = this;
                bool? result = d.ShowDialog();
                if (result.HasValue && result.Value == true)
                {
                    _repos.SavePerson(writeablePerson);
                }
            }
        }

        private void editButton_Click(object sender, RoutedEventArgs e)
        {
            IPerson person = peopleListBox.SelectedItem as IPerson;
            IPerson writeablePerson = _repos.GetWriteablePerson(person);
            {
                EditPersonDialog d = new EditPersonDialog(writeablePerson);
                d.Owner = this;
                bool? result = d.ShowDialog();
                if (result.HasValue && result.Value == true)
                {
                    _repos.SavePerson(writeablePerson);
                }
            }
        }

        private void deleteButton_Click(object sender, RoutedEventArgs e)
        {
            if (peopleListBox.SelectedItem == null)
                return;
            IPerson person = peopleListBox.SelectedItem as IPerson;
            _repos.DeletePerson(person);
        }

        private void bookListBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            editButton.IsEnabled = peopleListBox.SelectedItem != null;
            deleteButton.IsEnabled = peopleListBox.SelectedItem != null;
        }

        private void closeButton_Click(object sender, RoutedEventArgs e)
        {
            Close();
        }
    }
}
