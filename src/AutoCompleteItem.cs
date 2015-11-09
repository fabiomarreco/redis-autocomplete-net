namespace RedisAutocomplete.Net
{
    public struct AutoCompleteItem
    {
        public int Priority { get; set; }
        public string Text { get; set; }
        public string Item { get; set; }
    }
}