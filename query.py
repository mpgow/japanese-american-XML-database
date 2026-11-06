import sqlite3
import os
from dataclasses import dataclass, field 
# import fugashi # moved to db creation
# import pykakasi # moved to db creation
# import re # for regex (not used yet)
from xml_extraction_unicode import joinOCRChar, tokenizeTriplet

# Hard-coded, from directory of parent folder names
newspapers = {"jan" : "jan_NichibeiShinbun_Japanese American News",
            "kam": "kam_KashuMainichShinbun_The Japan California Daily News",
            "nwd" : "nwd_ShinSekai_The New World Daily News",
            "nws" : "nws_ShinSekai Asahi_The New World Sun",
            "tnj" : "tnj_Nippu Jiji",
            "tnw" : "tnw_ShinSekai_The New World"}

sentenceEnd = ".。?!！"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR,"test_newspaper_vers1.db")

# Filters determine which pages of entire database to search
@dataclass
class SearchFilters:
    startYear: int # required specificity
    endYear: int # required specificity
    sourceList: list = field(default_factory=list) # Automatically creates new list for each SearchFilters obj
    minConfidence: float = 0.0 # defaults to 0.0

# if tokens contain quotation marks, double it to make it SQL safe for querying
def ftsPhrase(tokens):
    return '"' + ' '.join(token.replace('"', '""') for token in tokens) + '"'

# returns the start indices where phrase is found in tokens
# only checks for entire phrase once first token of phrase matches current checked index
def phraseScanner(tokens, phrase):
    start = phrase[0] # first phrase token, use to qualify when to actually create a scanning slice
    found = [] # all indices that are the start of a fully matching phrase found in tokens
    for i in range(len(tokens)):
        if tokens[i] == start and tokens[i:i + len(phrase)] == phrase:
            found.append(i)
    return found

def lowercaseEach(tokens):
    return [token.lower() for token in tokens]

def inputPhrase(cursor):
    # phrase = "明治" # example single token kanji phrase
    phrase = input("what phrase or term are you searching for? ")
    # TODO: Do bilingual phrase search, so inputs can be in romaji or kanji, with case-sensitivity choice
    # inputs can be kanji, kana, romaji, or English
    # searching romaji or English just searches the input as typed without conversions
    surfacePhrase, kanaPhrase, romaPhrase = tokenizeTriplet(phrase)
    if not surfacePhrase:
        print(f"{phrase} has no searchable tokens")
        return None
    phraseLen = len(surfacePhrase)
    surfaceQuery = ftsPhrase(surfacePhrase)
    romaQuery = ftsPhrase(romaPhrase)

    # Need to take phrase, run it through tokenized Roma first to get superset's indices
    # tokenized Roma -> total hits from superset of romanized text
    # tokenizedText at the SAME index -> map of original text char : hit count
    # tokenizedText is also used to still do total occurrence count of EXACT phrase if desired
    print(f"phrase received: {phrase}")
    print(f"phrase tokens = {surfacePhrase} : romanized reading = {romaPhrase}")
    # pages row = (0:id, 1:filePath, 2:newspaper, 3:date, 4:pageNumber, 5:pageConfidence, 6:OCRSoftware, 7:text, 8:tokenizedText, 9:tokenizedKana, 10:tokenizedRoma)
    # fts row = (0:id, 1:tokenizedText, 2:tokenizedRoma)

    # General database overview for sense of scale
    cursor.execute("SELECT COUNT(*) FROM pages")
    print(f"Total pages = {cursor.fetchone()[0]}")
    cursor.execute("SELECT DISTINCT newspaper FROM pages")
    print(f"Newspapers = {[row[0] for row in cursor.fetchall()]}")
    cursor.execute("SELECT MIN(date), MAX(date) FROM pages")
    row = cursor.fetchone()
    print(f"Date range = {row[0]} to {row[1]}")

    # search romanized text for pages that contain the phrase, and then parse qualified pages to find exact indices and their writing to print out
    cursor.execute("SELECT COUNT(*) FROM pages_fts WHERE tokenizedRoma MATCH ?", [romaQuery]) # choose phrase here e.g. 二世 : nisei
    print(f"\nFTS page count search for romanized text \"{' '.join(romaPhrase)}\": {cursor.fetchone()[0]} results") # f's implicit rowid matched with p's explicit id
    cursor.execute("""SELECT SUBSTR(date, 1, 4) as year, COUNT(*)
                FROM pages_fts f JOIN pages p ON f.rowid = p.id
                WHERE f.tokenizedRoma MATCH ?
                AND date >= '1800-01-01' AND date <= '2040-12-31'
                GROUP BY year
                ORDER BY year ASC""", [romaQuery])
    print(f"\nFTS page frequency search for \"{phrase}\" results:")
    results = cursor.fetchall()
    if results:
        for row in results:
            year = row[0]
            count = row[1]
            print(f"Year {year}: {count} page appearances")
    else:
        print("No results found")
        return None

    # Go through the FTS qualified pages to count true occurrences
    # match indices using the romanized tokens, and then read paired surface tokens at those indices
    # this allows for broadband search and specific written form counts, without fugashi or pykakasi runs

    print(f"\nFTS frequency search for \"{phrase}\" results")
    cursor.execute("""SELECT p.tokenizedText, p.tokenizedRoma
                FROM pages p JOIN pages_fts f ON p.id = f.rowid
                WHERE f.tokenizedRoma MATCH ? 
                """, [romaQuery]) # need custom logic for searching true appearances
    counts = {}
    scanPhrase = lowercaseEach(romaPhrase)
    for surfaceText, romaText in cursor: # load rows of pages one at a time
        surfaceTokens = surfaceText.split()
        romaTokens = romaText.split()
        scanTokens = lowercaseEach(romaTokens)
        for i in phraseScanner(scanTokens, scanPhrase):
            counts[' '.join(surfaceTokens[i:i + phraseLen])] = counts.get(' '.join(surfaceTokens[i:i + phraseLen]), 0) + 1
    print(f"{sum(counts.values())} total appearances of shared case insensitive romanized text")
    for pKey, pValue in counts.items():
        print(f"{pKey}: {pValue}")
    print(f"{counts.get(' '.join(surfacePhrase), 0)} total appearances matched your exact input (case and transliteration sensitive)")
    return romaPhrase, surfacePhrase

# TODO: Refine search results
def collectFilters():
    while True:
        dateRange = input("Type in a date range in the form XXXX-XXXX: ")
        startYear, endYear = dateRange[0:4], dateRange[5:] # TODO: needs more specificity for edge cases later
        if len(dateRange) == 9 and startYear.isdigit() and endYear.isdigit():
            break
        print("incorrect date format, try again")
    startYear, endYear = int(startYear), int(endYear)

    print("Options for newspapers are: jan, kam, nwd, nws, tnj, tnw") # Hard-coded these values, can extract from UNIQUE values of 'newspaper' column in pages table
    sourceRange = input("Select newspapers if you'd like to filter them: {X Y Z... | '' }").split()
    while any (n not in newspapers for n in sourceRange):
        print("input must be whitespace separated newspapers")
        sourceRange = input("Select newspapers if you'd like to filter them: {X Y Z...}").split()
    sourceList = []
    for newspaper in sourceRange:
        sourceList.append(newspapers[newspaper])

    while True:
        pageConfidence = input("Select minimum page confidence 0 <= X <= 1.000 or blank for 0: ")
        try:
            pageConfidence = float(pageConfidence) if pageConfidence else 0.0
        except ValueError:
            print("That's not a number, try again")
            continue
        if 0 <= pageConfidence <= 1:
            break
        print("please check input inclusivity between 0 and 1")

    exact = input("Transliteration Modes: { B = broad, any written form | E = exact, match spelling exactly} ")
    while exact not in ("B", "E"):
        print("input must be B or E, try again")
        exact = input("Transliteration Modes: { B = broad, any written form | E = exact, match spelling exactly} ")
    exact = (exact == "E")

    index = input("Type in the numerical index of the sentence you'd like to see the phrase used in: ")
    while (not index.isdigit()):
        print("input must be an integer, try again")
        index = input("Type in the numerical index of the sentence you'd like to see the phrase used in: ")
    index = int(index)

    sensitive = input("Would you like to match case-sensitively? { Y | N } ")
    while (sensitive != "Y" and sensitive != "N"):
        print("input must be a single character, try again")
        sensitive = input("Would you like to match case-sensitively? { Y | N } ")
    sensitive = (sensitive == "Y")
    return startYear, endYear, sourceList, exact, index, sensitive, pageConfidence

# handle SQL filters
def buildFilters(filters): # startYear, endYear, sourceList, minConfidence
    clauses = ["CAST(SUBSTR(p.date, 1, 4) AS INTEGER) BETWEEN ? AND ?",
               "p.pageConfidence >= ?"]
    parameters = [filters.startYear, filters.endYear, filters.minConfidence]
    if filters.sourceList: # None means include all newspapers
        clauses.append(f"p.newspaper IN ({','.join('?' * len(filters.sourceList))})")
        parameters.extend(filters.sourceList)
    return " AND ".join(clauses), parameters

# TODO: Deprecate the original search method in favor of dual search version
def query(cursor, romaPhrase, surfacePhrase, startYear, endYear, sourceList, exact, index, sensitive, pageConfidence):
    # Find the exact sentence at the selected index
    # Broad "B" mode searches roma column (the superset); exact "E" mode searches the surface column (matching raw)

    phraseLen = len(surfacePhrase)
    surfaceQuery = ftsPhrase(surfacePhrase)
    romaQuery = ftsPhrase(romaPhrase)

    matchColumn = "tokenizedText" if exact else "tokenizedRoma"
    matchQuery = surfaceQuery if exact else romaQuery
    matchPhrase = surfacePhrase if exact else romaPhrase
    if not sensitive:
        matchPhrase = lowercaseEach(matchPhrase)

    # Ensure index ordering with ORDER BY clause, and narrow search with year filter
    where, parameters = buildFilters(SearchFilters(startYear, endYear, sourceList, pageConfidence))
    cursor.execute(f"""SELECT p.tokenizedText, p.tokenizedRoma, p.date
                    FROM pages p JOIN pages_fts f ON p.id = f.rowid
                    WHERE f.{matchColumn} MATCH ? AND {where}
                    ORDER BY p.date, p.id""", [matchQuery] + parameters)
    selectedTotal = 0
    found = False
    for surfaceText, romaText, SDate in cursor:
        surfaceTokens = surfaceText.split() # the original text that will be displayed
        matchTokens = surfaceTokens if exact else romaText.split() # the tokens our phrase will match check with
        if not sensitive:
            matchTokens = lowercaseEach(matchTokens) # only English changes, doesn't affect Japanese characters
        for i in phraseScanner(matchTokens, matchPhrase):
            if selectedTotal == index:
                sentence = []
                # lI scans left until reaches beginning of page or the end of the last sentence
                # rI scans right from the end of the found phrase until reaches the end of  page or the start of the next sentence 
                lI, rI = i - 1, i + phraseLen 
                while lI >= 0 and surfaceTokens[lI] not in sentenceEnd: # fugashi tokenizes punctuation by itself
                    # TODO: check if any character in the token is an ending punctuation
                    sentence.append(surfaceTokens[lI])
                    lI -= 1
                sentence = sentence[::-1] # reverse sentence because we scanned from right to left
                sentence.extend(surfaceTokens[i:i + phraseLen]) # add each token of the query phrase
                while rI < len(surfaceTokens):
                    sentence.append(surfaceTokens[rI])
                    if surfaceTokens[rI] in sentenceEnd: # Need to specify more punctuation
                        # get all consecutive ending punctuation before breaking out of the sentence
                        if rI + 1 >= len(surfaceTokens) or surfaceTokens[rI + 1] not in sentenceEnd:
                            break
                    rI += 1
                print(f"[matched as: {' '.join(surfaceTokens[i:i + phraseLen])}] {SDate}")
                print(joinOCRChar(sentence))
                found = True
                break
            selectedTotal += 1
            print(f">>>{SDate} aka {int(SDate[0:4])} with selected total: {selectedTotal}<<<")
            # Might want to print confidence of that specific page as a sanity point
            # Also, want to reduce counting in terminal prints due to line scroll limits
        if found:
            break
    if not found:
        print(f"only {selectedTotal} matches in that range, so index {index} is out of range") 

def main(db = DB_PATH):
    connection = sqlite3.connect(db)
    cursor = connection.cursor()
    # tagger = fugashi.Tagger()
    # kks = pykakasi.kakasi()

    while True:
        result = inputPhrase(cursor)
        if result is None:
            continue
        romaPhrase, surfacePhrase = result
        startYear, endYear, sourceList, exact, index, sensitive, pageConfidence = collectFilters()
        query(cursor, romaPhrase, surfacePhrase, startYear, endYear, sourceList, exact, index, sensitive, pageConfidence)
        cont = input("{ Exit | Continue }: ")
        while cont not in ("Exit", "Continue"):
            cont = input("{ Exit | Continue }: ")
        if cont == "Exit":
            connection.close()
            return
        else:
            continue

if __name__ == "__main__":
    main()
