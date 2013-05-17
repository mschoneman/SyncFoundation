using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BookSample.Data.Interfaces
{
    public interface IBook
    {
        string Title { get; set; }
        IList<IPerson> Authors { get; }
    }
}
