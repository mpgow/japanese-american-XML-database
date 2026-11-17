# test_consistency.py -- run with:  python test_consistency.py     (or:  pytest test_consistency.py)
# Placed next to query.py. Run it after changing getOccurrenceCounts() or iterMatches(), 
# to make sure the counts block and the sentence search still agree.
# That agreement lets main() reuse sum(counts.values()) as the match total in broad + case-insensitive mode.
import sqlite3
import query

# (typed phrase, start year, end year, newspaper codes). Edit these to phrases that exist in your DB.
# Keep them reasonably narrow: the test does full scans, so nws + one year is fast and a whole decade of every paper is not.
CASES = [("二世", 1940, 1940, ["nws"]),
         ("二世", 1935, 1935, ["tnj"]),
         ("二世", 1930, 1940, ["nws", "tnj"])]

def check(text, startYear, endYear, papers):
    connection = sqlite3.connect(query.DB_PATH)
    phrase = query.parsePhrase(text)
    filters = query.SearchFilters(startYear, endYear, [query.newspapers[p] for p in papers])
    options = query.SearchOptions()  # broad + case-insensitive: the one mode where main() takes its total from the counts block

    counts = query.getOccurrenceCounts(connection, phrase, filters)
    counted = sum(counts.values())
    scanned = query.countMatches(connection, phrase, filters, options)
    assert counted == scanned, f"{text!r}: the counts block says {counted} but iterMatches finds {scanned}" # displays repr of text, aka '二世' rather than 二世

    if scanned:  # every index the user is allowed to type must exist, and none beyond it
        assert query.getSentences(connection, phrase, filters, options, offset=scanned - 1), f"{text!r}: the last index ({scanned - 1}) has no sentence"
        assert not query.getSentences(connection, phrase, filters, options, offset=scanned), f"{text!r}: there are more matches than the total {scanned}"
    connection.close()
    return counts

def test_counts_block_matches_sentence_search():
    for n, case in enumerate(CASES):
        counts = check(*case)
        # print(f"\nPassed case #{n}: {case} with counted == scanned of {sum(counts.values())}") # for full written form : count breakdown
        query.showCounts(counts, query.parsePhrase(case[0]))

if __name__ == "__main__":
    test_counts_block_matches_sentence_search()
    print("OK: the counts block and the sentence search agree")
