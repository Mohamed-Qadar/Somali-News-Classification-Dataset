from keyword_sets import ECONOMY_KEYWORDS

SOURCES = [
    
    # POLITICS
  
    {
        "name": "Caasimada",
        "label": "Politics",
        "type": "wp_category",
        "base_url": "https://www.caasimada.net/category/wararka/",
        "pagination": "wp",
        "max_pages": 200000,
        "selectors": ["h1 a", "h2 a", "h3 a", ".entry-title a", ".post-title a"],
    },

    
    # WORLD
    
    {
        "name": "Caasimada",
        "label": "World",
        "type": "wp_category",
        "base_url": "https://www.caasimada.net/category/caalamka/",
        "pagination": "wp",
        "max_pages": 200000,
        "selectors": ["h1 a", "h2 a", "h3 a", ".entry-title a", ".post-title a"],
    },

    # SPORTS
    
    {
        "name": "Kooxda",
        "label": "Sports",
        "type": "wp_category",
        "base_url": "https://kooxda.com/category/wararka-ciyaaraha-maanta/",
        "pagination": "wp",
        "max_pages": 200000,
        "selectors": ["h1 a", "h2 a", "h3 a", ".entry-title a", ".post-title a"],
    },

    
    # ECONOMY (category)
    
    {
        "name": "RadioMuqdisho",
        "label": "Economy",
        "type": "keyword_archive",
        "base_url": "https://radiomuqdisho.so/category/wararka/",
        "pagination": "wp",
        "max_pages": 300000,
        "selectors": ["h1 a", "h2 a", "h3 a", ".entry-title a", ".post-title a", "article h2 a", "article h3 a"],
        "keywords": ECONOMY_KEYWORDS,
    },
    {
        "name": "RadioWaamo",
        "label": "Economy",
        "type": "keyword_archive",
        "base_url": "https://radiowaamo.so/category/wararka/",
        "pagination": "wp",
        "max_pages": 300000,
        "selectors": ["h1 a", "h2 a", "h3 a", ".entry-title a", ".post-title a", "article h2 a", "article h3 a"],
        "keywords": ECONOMY_KEYWORDS,
    },
    {
        "name": "Hiiraan",
        "label": "Economy",
        "type": "keyword_archive",
        "base_url": "https://www.hiiraan.com/wararkamaanta.php?page=",
        "pagination": "param",
        "start_page": 1,
        "max_pages": 300000,
        "selectors": ["h1 a", "h2 a", "h3 a", "h4 a"],
        "keywords": ECONOMY_KEYWORDS,
    },

   
    # ECONOMY (high-precision / low-volume sources)
  
    {
        "name": "SomaliWikipedia",
        "label": "Economy",
        "type": "keyword_archive",
        "base_url": "https://so.wikipedia.org/wiki/Category:Dhaqaale",
        "pagination": "param",   # You may later implement Wikipedia paging via "pagefrom" (advanced).
        "start_page": 1,
        "max_pages": 1,
        "selectors": ["#mw-pages a"],  # Simple category members extraction
        "keywords": [],  # Not needed; category already implies topic
    },
    {
        "name": "SomaliChamber",
        "label": "Economy",
        "type": "keyword_archive",
        "base_url": "<PUT_WORKING_SOMALICHAMBER_ARCHIVE_URL_HERE>",
        "pagination": "wp",
        "max_pages": 200000,
        "selectors": ["h1 a", "h2 a", "h3 a", ".entry-title a", ".post-title a"],
        "keywords": ECONOMY_KEYWORDS,
    },
    {
        "name": "PuntlandMOF",
        "label": "Economy",
        "type": "keyword_archive",
        "base_url": "<PUT_WORKING_PUNTLANDMOF_NEWS_OR_ARCHIVE_URL_HERE>",
        "pagination": "wp",
        "max_pages": 200000,
        "selectors": ["h1 a", "h2 a", "h3 a", ".entry-title a", ".post-title a"],
        "keywords": ECONOMY_KEYWORDS,
    },
]
