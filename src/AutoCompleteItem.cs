namespace RedisAutocomplete.Net
{
    public struct AutoCompleteItem
    {
        public int Priority { get; set; }
        public string Text { get; set; }
        public string ItemKey { get; set; }
        public string ItemContent { get; set; }
    }
}