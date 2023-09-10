from bs4 import BeautifulSoup
from requests import get
import json, time, csv
from pprint import pprint
import concurrent.futures

Website_URI = "https://apps.who.int"
counter = 1
Scape_URl = f'https://apps.who.int/iris/discover?rpp=10&etal=0&query=indigenous+communities&group_by=none&page=1&filtertype_0=dateIssued&filtertype_1=publisher&filtertype_2=iso&filter_relational_operator_1=equals&filter_relational_operator_0=equals&filter_2=English&filter_1=World+Health+Organization&filter_relational_operator_2=equals&filter_0=2021'

# Function to get all IRIS URL
def list_IRIS_URL():
    def get_page_uri(URI):
        content = get(URI)
        soup = BeautifulSoup(content.text, 'html.parser')
        IRIS_list = soup.find_all('div', class_='row ds-artifact-item')
        return list(map(lambda IRIS: Website_URI + IRIS.find("a").attrs['href']+"?show=full", IRIS_list))

    print("Getting IRIS URL")
    IRIS_list = []
    for page in range(1, 35):
        url = Scape_URl.replace("page=1", f"page={page}")
        IRIS_list.extend(get_page_uri(url))
        if page==5:
            break
    print("Total IRIS: ", len(IRIS_list))
    return IRIS_list

# Function to get meta data for each IRIS
def get_meta_data(IRIS_URL):
    print("Getting Meta Data for: ", IRIS_URL)
    content = get(IRIS_URL)
    soup = BeautifulSoup(content.text, 'html.parser')
    meta_data_table = soup.find("table")
    meta_data = {}
    for row in meta_data_table.find_all("tr"):
        td_list = row.find_all("td")
        meta_data[td_list[0].text] = td_list[1].text
    file = soup.find("div", class_="file-wrapper row").find("a").attrs['href']
    meta_data['file'] = Website_URI+file
    print("Completed meta data scraping for: ", IRIS_URL)
    return meta_data

# Main function
def main():
    list_IRIS = list_IRIS_URL()
    time.sleep(5)
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            result = list(executor.map(get_meta_data, list_IRIS))
    f = open("IRIS_336.csv", "w", newline="")
    csv_writer = csv.writer(f)
    csv_writer.writerow(["S.No", "Year", "Title", "WHO Setting", "Type of Document", "Department", "Technical Team", " DOCUMENT LINK", "link to search page"])
    for meta_data in result:
        try:
            csv_writer.writerow([result.index(meta_data)+1,"2021",meta_data['dc.title'],meta_data['dc.contributor.author'], meta_data['dc.type'],"","",meta_data['file'],meta_data['dc.identifier.uri']])
        except Exception as e:
            print("Error while writing: ", meta_data, e)
main()