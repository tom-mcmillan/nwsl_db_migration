# Scraping Fbref — Creating a Pipeline

Henrik Schjøth

10 min read · Feb 15, 2025

![](public/1_BLH53J7C1mhN2L8KP3GE9w.webp)

In this guide, we will scrape match data from Fbref and build a full web
scraping pipeline using BeautifulSoup and Selenium. We’ll start by scraping
a single table and gradually scale up to scraping all teams and two seasons of
match data using a combination of BeautifulSoup and Selenium. Below is the
final result.

![Two seasons from match data from](public/1_6heHretozyZrAnefvJJMjw.webp)



## Why is web scraping useful?

When I discovered web scraping, I thought it was only for semi‑professional
hackers. I was wrong, and now I can do it too! The process of learning has
included a lot of trial and error and a lot of time spent on something that I’ve
since discovered can be done much easier. Web scraping allow us to quickly
retrieve specific data, for any project we have in mind. That data can be
anything, from biological science to Oscar‑winners. In football analytics we
often collect different data to analyze teams, players or matches. We can get
market values from Transfermarkt while Sofascore, Fotmob and Fbref have
match‑data and aggregated event data. Here I’ll share strategies that can
automate the process and gets the tables you are searching for. There are
certainly more useful tips that can be added, feel free to let me know! Let’s
jump into it!

## Get one table —Using BeautifulSoup

Lets break it down and start with extracting one table from one team page.

![Squad page for Leeds](public/1_30hAZ7j3v8vt3OG38SokAw.webp)

First, we need to determine which information we want to extract,
specifically which table. The aim for this project is retrieving match results,
xG, xGA for each gameweek.

![](public/1_r2YW0HWCL3nEqc5ijXxSDw.webp)

Table with match data

```python
import requests
import pandas as pd
import warnings
# Hide FutureWarnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# URL for Leeds
team_url = "https://fbref.com/en/squads/5bfb9659/Leeds-United-Stats"

# Sendeing HTTP GET-request to webpage
data = requests.get(team_url)

# check for success response (statuscode 200)
if response.status_code == 200:
    print("Successfully retrieved page")
else:
    print(f"Failed to retrieve page. Status code: {response.status_code}")
```

## Breakdown of code
- Importing libraries
- Team-url (I used Leeds)
- Sending request to page
- Check for response from webpage


```python
from bs4 import BeautifulSoup
# Create a BeautifulSoup-objekt for navigating in HTML
soup = BeautifulSoup(data.text, "html.parser")

# Print first 500 characters to get an overview
print(soup.prettify()[:500])
```

![](public/1_Ol8WB5G9ljQYYcOKT3DJNQ.webp)

- Using BeautifulSoup to create an object, soup
- Printing first 500 charachters to get an overview

Now we have to find the table int the HTML. We can use inspect to find the table we wuld like to get. In this case it is the scores and figures-table. Right click and inspect allow to see how this look like in HTML.

![](public/1_nzS6ggCGhSHm637MgULNSw.webp)

We can use the select function in top corner to find the different elements in the HTML-code.

![](public/1_tk2O4CdDunOsJZfRX7fQ_Q.webp)

![](public/1_hLnHG0j9ZuEpf1nHybUIsg.webp)

We can hover over to find the table. Notice the class and id. This is the information we need to get the table. Both class and id can be used to find the table. In this case I have used id.

```python
# finding all tables in page
tables = soup.find_all("table") # finding table-elements
# print all ids for each table
for table in tables:
    print(table.get("id")) #print all ids

stats_standard_10
matchlogs_for
stats_keeper_10
stats_keeper_adv_10
stats_shooting_10
stats_passing_10
stats_passing_types_10
stats_gca_10
stats_defense_10
stats_possession_10
stats_playing_time_10
stats_misc_10
results2024-2025101_overall
results2024-2025101_home_away
```

### Breakdown

- Using soup-method -> find_all-> table elements
- Get all the table “id”
- Printing all the ids

![](public/1_F0pW05V6dDxbFzyTZk630A.webp)

The table we want have **“matchlogs_for”** as id. We can use a method
“select” from soup, which is called a CSS-selector. It selects elements which
match the description, in this case the “matchlogs_for” as id.

```python
table= soup.select("#matchlogs_for") #using '#' to indicate id we want 
table= table[0] #soup creates lists, so get first element
df=pd.read_html(str(table))[0]
df.head()

#another strategy using select
#table_two=soup.select("table#matchlogs_for") #finding tag(table) and id("matchlogs_for")
#table_two=table_two[0]
#df=pd.read_html(str(table_two))[0]
```

### Breakdown

- We use **soup.select** to find the table by its ID (matchlogs_for). **Select()** returns a list
- We select first element from list
- Convert element into a Pandas DataFrame using pd.read_html()
- Printing first five rows

![](public/1_ka4RrJQvK1wNP9ugBL4Lsg.webp)

```python
df.columns 
Index(['Date', 'Time', 'Comp', 'Round', 'Day', 'Venue', 'Result', 'GF', 'GA', 'Opponent', 'xG', 'xGA', 'Poss', 'Attendance', 'Captain', 'Formation', 'Opp Formation', 'Referee', 'Match Report', 'Notes'],
    dtype='object')
```

Here is columns from the dataframe with statistics like results and xG. Perfect for a machine learning prediction project!

In this example the table was **already present in the HTML source**, and BeautifulSoup and requests made it easy to retrieve the table quickly, without launching a browser. This is an effective and fast strategy. BeautifulSoup is great for static HTML, but if the page loads content dynamically, we need **Selenium**. Unfortunately many modern websites **dynamically** load content using **JavaScript**, meaning some elements (like tables, buttons, or text) are **not present** in the initial **HTML source** when you request the page using requests or BeautifulSoup. Instead, the page executes JavaScript to fetch and display the content **after the page has initially loaded**. Luckily Selenium can solve this problem. Here is an example of Selenium getting the same table as above.

## Get One table — Using Selenium

```python
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd

# Setup for Selenium
chrome_options = Options()
chrome_options.add_argument("--headless")  
chrome_options.add_argument("--disable-gpu")
service = ChromeService(executable_path=r"C:\Users\henri\Documents\Chromedriver\chromedriver-win64\chromedriver.exe") # add path to driver
driver = webdriver.Chrome(service=service, options=chrome_options)


team_url = "https://fbref.com/en/squads/5bfb9659/Leeds-United-Stats" # Add team url

# load page
driver.get(team_url)

try:
    # wait for page to load 
    wait = WebDriverWait(driver, 10)  # waiting max 10sec, can be adjusted
    table = wait.until(EC.presence_of_element_located((By.ID, "matchlogs_for")))

    html_table = table.get_attribute('outerHTML')

    # Convert HTML to pandas DataFrame
    matches = pd.read_html(html_table)[0]
    print(matches.head())  
except Exception as e:
    print(f"Error: {e}")
finally:
    driver.quit()  
```

### Breakdown

- Starts Selenium in headless mode (does not open up a browser window).
- Loads the webpage for Leeds United’s statistics.
- Waits for a specific table (“matchlogs for”) to appear.
- Extracts the table’s HTML code.
- Converts it into a Pandas DataFrame.
- Handles errors and closes the browser.

### Why not only use Selenium?

- Selenium is slow — it loads the whole page, including JavaScript, CSS and images
- Selenium using a real browser — could be easier to detect and blocked

We have doubled our repertoire! For doing that we need to be able to navigate from leaguepage to teampage. Now that we’ve successfully extracted data from one team, let’s scale up our pipeline to scrape data from all teams in the league. To do this, we first need to retrieve the links to each team’s page.

## Navigate From Leaguepage to Teampage

![](public/1_QnEKYjRmg3q2fZZq5eUTTg.webp)

We start from the main leaguepage. We want to access the team links in this table.

```python
standings_url = "https://fbref.com/en/comps/10/Championship-Stats" #league-url
data = requests.get(standings_url) 
soup = BeautifulSoup(data.text) 
standings_table = soup.select('table.stats_table')[0]
```

The first steps is same as we started with in teampage. Using request soup.select to get the table.

![](public/1_JdXmKDao05CrQTb3WHBb2Q.webp)

We want to find the team links. Inspect give us information about the different team links. In HTML, the `<a>` tag (anchor tag) is used to create a hyperlink. The **href** attribute (Hypertext Reference) specifies the URL that the link points to.

![](public/1_kxOgsUFH1-KhYWx48KilwA.webp)

```python
links = standings_table.find_all('a') # finding the "a"-tags
links # print to check 
links = [l.get("href") for l in links] 
links = [l for l in links if '/squads/' in l] # filtering for squad links
links #print to check again
team_urls = [f"https://fbref.com{l}" for l in links] # adding text to complete links 
```

### Breakdown

- Using soups find_all method -> looking for elements with an “a” tag.
- Printing links

![](public/1_EXgLLUZ9bP09rMZZHQl2qw.webp)

- Using list-comprehension to get all links, which is the href — property
- Filtering all links to get only those containing squads
- Printing again to check

![](public/1_kpdlsxbZBMWRNh2rPeBaqA.webp)

- Adding the start of the link containing the domain ( Links are missing the beginning)

![](public/1_mut97moQs5T4_mbBvaih-A.webp)

Now we have the full links to all teams in the league and can access them by using team_urls[“linknumber”]. We can access the table from any team in the league, but doing that manually will take hours.

```python
team_name= team_urls[0].split("/")[-1].replace("-Stats", "").replace("-", " ") # finding team name
data = requests.get(team_urls[0]) #checking data from first teamurl (which is leeds, same url as code in beginning)
# Create a BeautifulSoup-objekt for navigating in HTML
soup = BeautifulSoup(data.text, "html.parser") # now we are back to where we began
table= soup.select("#matchlogs_for") #using '#' to indicate id we want 
table= table[0] #soup creates lists, so get first element
df=pd.read_html(str(table))[0]
```

Now that we have successfully retrieved team links and extracted match data from one team, let’s scale up and scrape all teams in the league.

## Creating a full webscraper

To create a full webscraper we need a function that does all the above, looping through all teams and collecting tables. After trying this with BeautifulSoup, i discovered some match-tables were missing. Therefore I combined BeautifulSoup and Selenium to get all matches. This code first search for tables with BeautifulSoup. If the page is loaded dynamically, we use Selenium for that particular table. It loops through all team links and we can put in a list with years which make it possible to get data from multiple seasons for one league.

```python
# # add the years you want to scrape. 
years = list(range(2024, 2022, -1)) # Starts with 2024 season, ends with 2023 season
all_matches = [] #create empty list for dataframes
standings_url = "https://fbref.com/en/comps/9/Premier-League-Stats" # league url
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
} #headers make the request look like it come from a user-agent. Some sites demand headers
```

```python
# combination of beautiful soup and selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import requests
import time

# setup for Selenium
chrome_options = Options()
chrome_options.add_argument("--headless")  
chrome_options.add_argument("--disable-gpu")
service = ChromeService(executable_path=r"C:\Users\henri\Documents\Chromedriver\chromedriver-win64\chromedriver.exe") # add path to driver here
driver = webdriver.Chrome(service=service, options=chrome_options)

for year in years:
    data = requests.get(standings_url,headers=headers)
    soup = BeautifulSoup(data.text, 'html.parser')
    standings_table = soup.select('table.stats_table')[0]
    links = [l.get("href") for l in standings_table.find_all('a')]
    links = [l for l in links if '/squads/' in l]
    team_urls = [f"https://fbref.com{l}" for l in links]

    # Finding previous season
    previous_season = soup.select("a.prev")[0].get("href")
    standings_url = f"https://fbref.com{previous_season}"

    for team_url in team_urls:
        team_name = team_url.split("/")[-1].replace("-Stats", "").replace("-", " ")
        print(f'Scraping {team_name} for season {year}')
        
        # Try first using BeautifulSoup
        data = requests.get(team_url,headers=headers)
        soup = BeautifulSoup(data.text, 'html.parser')
        table = soup.find('table', id='matchlogs_for')
        
        if table:  # Checking if table exists in HTML
            matches = pd.read_html(str(table))[0]
            matches["Season"] = year
            matches["Team"] = team_name
            all_matches.append(matches)
        else:  # If table does not exist in HTML, using Selenium
            print(f"Using Selenium for {team_name}") 
            driver.get(team_url)
            try:
                # adding waiting time to load page
                wait = WebDriverWait(driver, 10)  # Set max waiting time to 10 sec
                selenium_table = wait.until(EC.presence_of_element_located((By.ID, "matchlogs_for")))
                html_table = selenium_table.get_attribute('outerHTML')

                # Convert table to pandas DataFrame
                matches = pd.read_html(html_table)[0]
                matches["Season"] = year
                matches["Team"] = team_name
                all_matches.append(matches)
            except Exception as e:
                print(f"Failed to scrape {team_name} with Selenium: {e}")
        
        # Wait to avoid blocking
        time.sleep(6)

# Combine all dataframes 
driver.quit()  # Close Selenium-driver
all_seasons = pd.concat(all_matches, ignore_index=True)
all_seasons.sort_values(by="Date")

# Check result
print(all_seasons.head())
```

1. Sets up Selenium (Headless Chrome)
2. Loops through each season `(years)`
3. Fetches the league table `(standings_table)` using BeautifulSoup
4. Collects links to all teams `(`team_urls)`
5. Finds the link to the previous season for the next iteration
6. Loops through each team `(team_urls)`
7. Fetches the team’s statistics page
8. Attempts to find the `matchlogs_for` table using BeautifulSoup first
9. If BeautifulSoup fails, uses Selenium to retrieve it dynamically
10. Converts the table into a Pandas DataFrame and adds it to `all_matches`
11. Combines all season data into a single DataFrame `(all_seasons)`
12. Closes Selenium `(driver.quit())` to free up system resources
13. Displays the first few rows of `all_seasons`

🚀 This makes the scraping process both efficient and reliable!

![](public/1_a6heHretozyZrAnefvJJMjw.webp)

## Review the DataFrame
Sometimes the results is different to what we expected. That makes verifying and cleaning the data important before using it. Filtering for only matches played and check for number of matches.

![](public/1_Dke8E6kdrhuoZddpVg7-SA.webp)

```python
len(all_seasons) #checking number of rows
# removing other competitions, keep only championsshipp games
all_seasons = all_seasons.loc[all_seasons['Comp'] == 'Championship']
df_filtered= all_seasons[all_seasons["Date"]<"2025-02-14"] #filtering for today, keeping only matches played
```

```python
# finding real number of games - checking that we got all games (some teams had played 31 and some 32 matches)
number_matches=31*8 +32*16 
number_matches_df= len(df_filtered) #check number of matches
print(f'number of games played: {number_matches}')
print(f'number of matches in DataFrame: {number_matches_df_filtered}')
```

![](public/1_UYh7U85wE_dUdnBVDmMCfA.webp)

### To summarize

We have now built a full web scraping pipeline that extracts match data for multiple seasons using a combination of BeautifulSoup and Selenium. By prioritizing BeautifulSoup when possible, we keep the process efficient, and by using Selenium only when necessary, we ensure we get all data even when JavaScript is involved. The data is now ready to be used, for example in a machine learning project. For this article I have been inspired from various ressources. There are definately more techniques and advanced tips and tricks which can be added, feel free to let me know ! Good luck with scraping!

### References

https://medium.com/@conalhenderson/how-to-build-a-custom-web-scraper-to-extract-premier-league-player-market-data-3b8e5378cca2

https://ricardoheredia94.medium.com/scraping-fbref-for-data-driven-scouting-and-enhanced-player-profiling-464acad83270

https://github.com/dataquestio/project-walkthroughs/tree/master/football_matches
