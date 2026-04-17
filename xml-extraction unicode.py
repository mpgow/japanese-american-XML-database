import sqlite3 # SQL database
import xml.etree.ElementTree as ET # python API for parsing XML trees
import os # used for walking through directory to parse files
import fugashi

tagger = fugashi.Tagger()

# Defines the relative starting location for directory search to be where the script file is located,
# so that script should still work regardless of working directory if executing in terminal.
# Otherwise, running in an IDE should be able to path resolve fine without this.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 

class file:
    # TODO: decide what metadata we should collect from the files
    # Technically will likely never initialize an entry with these values, except manually
    def __init__(self, filePath=None, newspaper=None, date=None, pageNumber=None, pageConfidence=None, OCRSoftware=None, text=None, tokenizedText=None):
        self.filePath = filePath             # from loop parse
        self.newspaper = newspaper           # from loop parse
        self.date = date                     # from extractFile
        self.pageNumber = pageNumber         # from extractFile
        self.pageConfidence = pageConfidence # from extractFile
        self.OCRSoftware = OCRSoftware       # from extractFile
        self.text = text                     # from extractFile
        self.tokenizedText = tokenizedText   # from extractFile, no redundant copy in pages

def createDatabase(dbname):
    # TODO: create database and call parsing function to create table
    connection = sqlite3.connect(f"{dbname}")
    cursor = connection.cursor()
    # filepath example: ...\UCB\nws_ShinSekai Asahi_The New World Sun\1940\05\05_01
    cursor.execute("""CREATE TABLE IF NOT EXISTS pages (
                   filepath TEXT NOT NULL,
                   newspaper TEXT,
                   date TEXT,
                   pageNumber INTEGER,
                   pageConfidence REAL,
                   OCRSoftware TEXT,
                   text TEXT
                   )""")
    # choose text column for indexing since that's likely what we'll do our phrase searches on
    # specify content location to avoid duplicating all of the database text locally
    # content_rowid uses implicit rowid that SQLite assigns each row
    # unicode splits on whitespace, fugashi splits logical phrases with whitespaces
    # join FTS5 table with original to link tokenizedText and other metadata
    # contentless table means tokenizedText thrown away after reverse-index generated
    # but can't use snippet() or highlight(), bc only position not text stored, join to recreate snippet
    cursor.execute("""CREATE VIRTUAL TABLE IF NOT EXISTS pages_fts
                   USING fts5(tokenizedText, content="", content_rowid="rowid", tokenize="unicode61")""")
    # EDIT r"UCB" VALUE TO CONFORM TO YOUR RELATIVE FOLDER LOCATION (note example above)
    rootLocation = os.path.join(SCRIPT_DIR, r"UCB") # smartly handles OS-dependent path creation
    alreadyIndexed = set() # good for re-running after a crash, skip already scanned files
    cursor.execute("SELECT filepath FROM pages")
    alreadyIndexed = {row[0] for row in cursor.fetchall()}
    batchSize = 100 # For every 100 files, upload and commit for crash safety (and while fitting in memory)
    batchPages = []
    batchFTS = []
    for row in directoryParse(rootLocation, alreadyIndexed):
        # row = (filePath, newspaper, date, pageNumber, pageConfidence, OCRSoftware, text, tokenizedText)
        batchPages.append(row[:7]) #exclude tokenizedText
        batchFTS.append(row[7]) # just tokenizedText
        if (len(batchPages)) >= batchSize:
            flushToDisk(cursor, batchPages, batchFTS)
            batchPages.clear()
            batchFTS.clear()
            connection.commit()
    if (len(batchPages) > 0): # at end of directory tree, flush any partial final batches
        flushToDisk(cursor, batchPages, batchFTS)
        connection.commit()
    connection.close()

def flushToDisk(cursor, batchPages, batchFTS):
    cursor.executemany("INSERT INTO pages VALUES (?, ?, ?, ?, ?, ?, ?)", batchPages) # raw string
    firstRowID = cursor.lastrowid - len(batchPages) + 1 # get the row ids assigned to pages, and match them to FTS entries
    i = 0
    ftsRows = []
    for row in batchFTS:
        ftsRows.append((firstRowID + i, row))
        i += 1
    cursor.executemany("INSERT INTO pages_fts(rowid, tokenizedText) VALUES (?, ?)", ftsRows) # pages_fts is basically an index on pages


def directoryParse(dirRootPath, alreadyIndexed):
    # TODO: loop through all newspapers, dates, and file pages to generate each full entry
    if (not os.path.isdir(dirRootPath)): 
        print(f"'{dirRootPath}' is not a valid directory.")
        return
    # collectedFiles = []
    for dirRoot, dirName, files in os.walk(dirRootPath):
        for fileName in files:
            if (fileName.lower().endswith("xml")):
                filePath = os.path.join(dirRoot, fileName)
                # checking if the filePath has already been inserted, if so then skip processing
                if (filePath in alreadyIndexed):
                    print(f"skip {filePath}")
                    continue
                # parts = dirRoot.split(os.sep) # useful split, selects separation character based on OS
                # currNewspaper = parts[1] # however, no longer works since I am adding root_location with script's relative location
                
                # dirRoot (fuller) includes everything in path except the final filename
                # dirRootPath (shorter) includes path up to \UCB, since we joined last that in root_location
                relPath = os.path.relpath(dirRoot, dirRootPath)
                relParts = relPath.split(os.sep) # split path into array based on OS path separators
                currNewspaper = relParts[0] # first subfolder in \UCB is the newspaper titled folders themselves
                cF = file(filePath=filePath, newspaper=currNewspaper) # current file being extracted
                try:
                    extractFile(entry=cF, filePath=filePath)
                except Exception as e:
                    print(f"Failed on {filePath}: {e}")
                    continue
                # TODO: add validation

                yield (cF.filePath, cF.newspaper, cF.date, cF.pageNumber, cF.pageConfidence, cF.OCRSoftware, cF.text, cF.tokenizedText) # yield generator to insert file by file
                # collectedFiles.append(extractFile(currFile, filePath)) // cannot build a list of 120GB of text and metadata in a normal machine's local memory
    # return collectedFiles


# Inserts all the page's text and relevant metadata into the collection database 
def extractFile(entry, filePath):
    print(f"starting extraction of {filePath}")
    ns = "{http://www.loc.gov/standards/alto/ns-v3#}" # implicit namespace before all tags
    # Take in a filepath and reads in XML data
    tree = ET.parse(filePath) # can be adjacent filename or specific filepath
    root = tree.getroot() # <alto>
    # Gather embedded metadata
    fnText = root.find(f"{ns}Description/{ns}sourceImageInformation/{ns}fileName").text # redundancy
    date = fnText[6:10]+'-'+fnText[10:12]+'-'+fnText[12:14] # example: ./nws_19350805_0002.xml -> 1935-08-05
    OCRSoftwareRoot = root.find(f"{ns}Description/{ns}OCRProcessing/{ns}ocrProcessingStep/{ns}processingSoftware")
    OCRSoftware = ' '.join([OCRSoftwareRoot.find(f"{ns}softwareName").text, OCRSoftwareRoot.find(f"{ns}softwareVersion").text])
    pageNumber = root.find(f"{ns}Layout/{ns}Page").get("PHYSICAL_IMG_NR")
    pageConfidence = root.find(f"{ns}Layout/{ns}Page").get("PC")
    # Find all strings within the page
        # print(root[2][0][4])
        # textRoot = root[2][0][4] # works, but much safer to specify via text
    textRoot = root.find(f"{ns}Layout/{ns}Page/{ns}PrintSpace") # steps into <alto>:[2]<Layout>[0]<Page>[4]<PrintSpace>
        # for textBlock in textRoot.iter(f"{ns}TextBlock"):
        #     print(textBlock)
        #     for textLine in textBlock.iter(f"{ns}TextLine"):
        #         print(textLine)
        #         for string in textLine.iter(f"{ns}String"):
        #             print(string)
        #             OCRitem = string.get("CONTENT") # attribute of each OCR chunk is called CONTENT
        #             print(OCRitem)
    strings = textRoot.iter(f"{ns}String") # ET iterator of all (nested) strings in textRoot
    # Concatenate strings into a single text
    text = ' '.join(s.get("CONTENT") for s in strings) # space separate each OCR "word"
    tokenizedText = ' '.join([word.surface for word in tagger(text)])
    print (text)
    # TODO: Run a tokenizer to speed up keyword searches in queries
        # May want to address OCR corruptions first
    # Inserts all metadata, raw text (for user readability),
    # (and nested tokenized words) into collection database 
    entry.date = date
    entry.OCRSoftware = OCRSoftware
    entry.text = text
    entry.tokenizedText = tokenizedText
    entry.pageNumber = pageNumber
    entry.pageConfidence = pageConfidence
    # return entry # don't need to return because we are modifying entry's values in place

# For testing

# extractFile(file(), "nws_19350805_0002.xml") # check that file's attributes are correctly scanned
# createDatabase("test_tnw_unicode_version.db") # As I run this, I only have the tnw_ShinSekai_The New World folder inside the relative directory \UCB
# createDatabase("test_tnw+nws_unicode_version.db") # As I run this, I only have the tnw_ShinSekai_The New World & nws_ShinSekai Asahi_The New World Sun folders inside the relative directory \UCB
createDatabase("test_newspaper.db") # As I run this, I only have the tnw_ShinSekai_The New World & nws_ShinSekai Asahi_The New World Sun folders inside the relative directory \UCB