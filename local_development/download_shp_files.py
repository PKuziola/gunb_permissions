import zipfile
import requests
from bs4 import BeautifulSoup


desired_unit_data_types = ['Województwa','Powiaty','Gminy','Jednostki ewidencyjne']
url = 'https://gis-support.pl/baza-wiedzy-2/dane-do-pobrania/granice-administracyjne/'
response = requests.get(url)
html = response.text
soup = BeautifulSoup(html, "html.parser")
soup_article_content = soup.find("div", {"class": "bpress-article-content"})

for a in soup_article_content.find_all('a', href=True):
    if a.text in desired_unit_data_types:
        link = a['href']
        r = requests.get(link)
        filename = link.split('/')[-1]
        with open(filename,'wb') as output_file:
            output_file.write(r.content)
        with zipfile.ZipFile(filename, 'r') as zip_file:
            file_names = zip_file.namelist()
            prefix = filename.split('.')[0]
            desired_file_name = f"{prefix}.shp"
            file_index = file_names.index(desired_file_name)
            zip_file.extract(file_names[file_index])




