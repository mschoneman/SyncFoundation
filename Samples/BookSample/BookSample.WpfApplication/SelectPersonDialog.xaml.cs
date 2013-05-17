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
    /// Interaction logic for SelectPersonDialog.xaml
    /// </summary>
    public partial class SelectPersonDialog : Window
    {
        BookRepository _repos;

        public SelectPersonDialog(BookRepository repos)
        {
            InitializeComponent();
            this._repos = repos;
            peopleListBox.ItemsSource = _repos.AllPeople;
        }

        public IPerson SelectedPerson
        {
            get
            {
                return peopleListBox.SelectedItem as IPerson;
            }
        }


        private void peopleListBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            okButton.IsEnabled = peopleListBox.SelectedItem != null;
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
    }
}
