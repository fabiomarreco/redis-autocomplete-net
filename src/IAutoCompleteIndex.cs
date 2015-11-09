using System.Collections.Generic;
using System.Threading.Tasks;

namespace RedisAutocomplete.Net
{
    public interface IAutoCompleteIndex
    {
        Task Add(params AutoCompleteItem[] items);

        Task<long> RemoveItem(params string[] items);

        Task IncreasePriority(string item);
        Task Clear();
        Task<string[]> Search(string searchTerm);
    }
}