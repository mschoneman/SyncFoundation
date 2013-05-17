using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BookSample.Data.Interfaces
{
    public interface IPerson : IBaseItem
    {
        string Name { get; set; }
    }
}
