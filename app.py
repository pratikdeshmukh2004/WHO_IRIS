from bs4 import BeautifulSoup
from requests import get
import json, time, csv
from pprint import pprint
import concurrent.futures
from urllib.parse import urlparse, parse_qs


# To be changed...
Website_URI = "https://apps.who.int"
Scape_URl = 'https://apps.who.int/iris/discover?filtertype_1=dateIssued&filter_relational_operator_1=equals&filter_1=2021&filtertype_2=publisher&filter_relational_operator_2=equals&filter_2=World+Health+Organization&filtertype_3=iso&filter_relational_operator_3=equals&filter_3=English&filtertype_4=type&filter_relational_operator_4=equals&filter_4=Technical+documents&submit_apply_filter=&query=Tribal+communities'
total_pages = 1

# Get query params
parsed_url = urlparse(Scape_URl)
query_params = parse_qs(parsed_url.query)

# Function to get all IRIS URL
def list_IRIS_URL():
    def get_page_uri(URI):
        content = get(URI)
        soup = BeautifulSoup(content.text, 'html.parser')
        IRIS_list = soup.find_all('div', class_='row ds-artifact-item')
        return list(map(lambda IRIS: Website_URI + IRIS.find("a").attrs['href']+"?show=full", IRIS_list))

    print("Getting IRIS URL")
    IRIS_list = []
    for page in range(1, total_pages+1):
        url = Scape_URl.replace("page=1", f"page={page}")
        IRIS_list.extend(get_page_uri(url))
    print("Total IRIS: ", len(IRIS_list))
    return IRIS_list

# Function to get meta data for each IRIS
def get_meta_data(IRIS_URL):
    content = get(IRIS_URL)
    soup = BeautifulSoup(content.text, 'html.parser')
    meta_data_table = soup.find("table")
    meta_data = {}
    for row in meta_data_table.find_all("tr"):
        td_list = row.find_all("td")
        if td_list[0].text in meta_data:
            meta_data[td_list[0].text] = td_list[1].text+", "+ meta_data[td_list[0].text]
        else:
            meta_data[td_list[0].text] = td_list[1].text
    file = soup.find("div", class_="file-wrapper row").find("a").attrs['href']
    meta_data['file'] = Website_URI+file
    return meta_data

# Main function
def main():
    list_IRIS = list_IRIS_URL()
    total_items = len(list_IRIS)
    errored = 0
    time.sleep(5)
    print(f"Scrapping {total_items} IRIS...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            result = list(executor.map(get_meta_data, list_IRIS))
    current_time = time.strftime("%H:%M:%S")
    fileName = f"DataFiles/{query_params['query'][0]}_{query_params['filter_1'][0]}_{current_time}.csv"
    f = open(fileName, "w", newline="")
    csv_writer = csv.writer(f)
    csv_writer.writerow(["S.No", "Year", "Title", "WHO Setting", "Type of Document", "Department", "Technical Team", " DOCUMENT LINK", "link to search page"])
    for meta_data in result:
        try:
            csv_writer.writerow([result.index(meta_data)+1,"2021",meta_data['dc.title'],meta_data['dc.contributor.author'], meta_data['dc.type'],"","",meta_data['file'],meta_data['dc.identifier.uri']])
        except Exception as e:
            errored += 1
            print("\n\nError while writing: ", meta_data['dc.identifier.uri'])
    print(f'\n\nScraped {total_items - errored}/{total_items}\nFile saved as: "{fileName}"')
main()