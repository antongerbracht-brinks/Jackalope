namespace Jackalope.Broker;

/// <summary>
/// AMQP topic-exchange matching. Keys and patterns are '.'-separated words; in a pattern '*' matches exactly
/// one word and '#' matches zero or more words. An empty string is zero words (so "#" matches "" but "*" does not),
/// while empty words between dots ("a..b") are real, matchable words, as in RabbitMQ.
/// </summary>
internal static class TopicMatcher
{
    public static bool IsMatch(string pattern, string routingKey) =>
        Match(Split(pattern), 0, Split(routingKey), 0);

    private static string[] Split(string value) => value.Length == 0 ? [] : value.Split('.');

    private static bool Match(string[] pattern, int p, string[] key, int k)
    {
        while (p < pattern.Length)
        {
            string word = pattern[p];
            if (word == "#")
            {
                // Collapse consecutive '#'s, then try every possible number of consumed words.
                while (p + 1 < pattern.Length && pattern[p + 1] == "#")
                {
                    p++;
                }

                if (p == pattern.Length - 1)
                {
                    return true;
                }

                for (int skip = k; skip <= key.Length; skip++)
                {
                    if (Match(pattern, p + 1, key, skip))
                    {
                        return true;
                    }
                }

                return false;
            }

            if (k == key.Length || (word != "*" && word != key[k]))
            {
                return false;
            }

            p++;
            k++;
        }

        return k == key.Length;
    }
}
