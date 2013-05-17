using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BookSample.Data.Interfaces
{
    public interface IBaseItem : INotifyPropertyChanged
    {
        bool IsReadOnly { get; }
        bool IsModified { get; }
    }
}
