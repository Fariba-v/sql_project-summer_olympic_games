```python
from pathlib import Path
Path('my_data.db').touch()
```


```python
import sqlite3
conn = sqlite3.connect('my_data.db')
c = conn.cursor()
```

c.execute('''CREATE TABLE users (user_id int, username text)''')



```python
import pandas as pd
# load the data into a Pandas DataFrame
users = pd.read_csv('businesses.csv')
# write the data to a sqlite table
users.to_sql('businesses', conn, if_exists='append', index = False)
```




    163




```python

```


```python
pd.read_sql('''

SELECT *
FROM businesses
WHERE year_founded < 999
ORDER BY year_founded

''', conn)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>business</th>
      <th>year_founded</th>
      <th>category_code</th>
      <th>country_code</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Kongō Gumi</td>
      <td>578</td>
      <td>CAT6</td>
      <td>JPN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Kongō Gumi</td>
      <td>578</td>
      <td>CAT6</td>
      <td>JPN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>St. Peter Stifts Kulinarium</td>
      <td>803</td>
      <td>CAT4</td>
      <td>AUT</td>
    </tr>
    <tr>
      <th>3</th>
      <td>St. Peter Stifts Kulinarium</td>
      <td>803</td>
      <td>CAT4</td>
      <td>AUT</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Staffelter Hof Winery</td>
      <td>862</td>
      <td>CAT9</td>
      <td>DEU</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Staffelter Hof Winery</td>
      <td>862</td>
      <td>CAT9</td>
      <td>DEU</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Monnaie de Paris</td>
      <td>864</td>
      <td>CAT12</td>
      <td>FRA</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Monnaie de Paris</td>
      <td>864</td>
      <td>CAT12</td>
      <td>FRA</td>
    </tr>
    <tr>
      <th>8</th>
      <td>The Royal Mint</td>
      <td>886</td>
      <td>CAT12</td>
      <td>GBR</td>
    </tr>
    <tr>
      <th>9</th>
      <td>The Royal Mint</td>
      <td>886</td>
      <td>CAT12</td>
      <td>GBR</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Sean's Bar</td>
      <td>900</td>
      <td>CAT4</td>
      <td>IRL</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Sean's Bar</td>
      <td>900</td>
      <td>CAT4</td>
      <td>IRL</td>
    </tr>
  </tbody>
</table>
</div>




```python

```


```python
c.execute('''SELECT * FROM businesses''').fetchall()
```




    [('Hamoud Boualem', 1878, 'CAT11', 'DZA'),
     ('Communauté Électrique du Bénin', 1968, 'CAT10', 'BEN'),
     ('Botswana Meat Commission', 1965, 'CAT1', 'BWA'),
     ('Air Burkina', 1967, 'CAT2', 'BFA'),
     ('Brarudi', 1955, 'CAT9', 'BDI'),
     ('Cameroon Development Corporation', 1947, 'CAT1', 'CMR'),
     ('Correios de Cabo Verde', 1849, 'CAT16', 'CPV'),
     ('Banque Internationale pour la Centrafrique', 1946, 'CAT3', 'CAF'),
     ('Cotontchad', 1971, 'CAT1', 'TCD'),
     ('Central Bank of the Comoros', 1981, 'CAT3', 'COM'),
     ('Société nationale des Chemins de fer du Congo', 1889, 'CAT2', 'COD'),
     ('Development Bank of the Central African States', 1975, 'CAT3', 'COG'),
     ('La Poste', 1945, 'CAT16', 'CIV'),
     ('Ethio-Djibouti Railways', 1901, 'CAT2', 'DJI'),
     ('Egyptian National Railways', 1854, 'CAT2', 'EGY'),
     ('Guinea Ecuatorial Airlines', 1996, 'CAT2', 'GNQ'),
     ('Asmara Brewery', 1939, 'CAT9', 'ERI'),
     ('National Bank of Ethiopia', 1906, 'CAT3', 'ETH'),
     ('BGFIBank Group', 1971, 'CAT3', 'GAB'),
     ('Halco Mining', 1962, 'CAT15', 'GIN'),
     ('Correios da Guiné-Bissau', 1973, 'CAT16', 'GNB'),
     ('KCB Group Limited', 1896, 'CAT3', 'KEN'),
     ('Central Bank of Lesotho', 1978, 'CAT3', 'LSO'),
     ('National Port Authority', 1921, 'CAT2', 'LBR'),
     ('Umma Bank', 1907, 'CAT3', 'LBY'),
     ('Air Madagascar', 1962, 'CAT2', 'MDG'),
     ('Malawi Broadcasting Corporation', 1964, 'CAT13', 'MWI'),
     ('Office de Radiodiffusion-Télévision du Mali', 1957, 'CAT13', 'MLI'),
     ('Central Bank of Mauritania', 1973, 'CAT3', 'MRT'),
     ('Mauritius Post', 1772, 'CAT16', 'MUS'),
     ('Attijariwafa Bank', 1904, 'CAT3', 'MAR'),
     ('Beira Railroad Corporation', 1892, 'CAT2', 'MOZ'),
     ('NamPost', 1814, 'CAT16', 'NAM'),
     ('Office of Radio and Television of Niger', 1960, 'CAT13', 'NER'),
     ('First Bank of Nigeria', 1894, 'CAT3', 'NGA'),
     ('National Post Office', 1922, 'CAT16', 'RWA'),
     ('Central Bank of São Tomé and Príncipe', 1975, 'CAT3', 'STP'),
     ('Dakar–Niger Railway', 1924, 'CAT2', 'SEN'),
     ('Air Seychelles', 1977, 'CAT2', 'SYC'),
     ('Rokel Commercial Bank', 1917, 'CAT3', 'SLE'),
     ('Radio Mogadishu', 1943, 'CAT13', 'SOM'),
     ('Premier FMCG', 1820, 'CAT12', 'ZAF'),
     ('Ivory Bank', 1994, 'CAT3', 'SSD'),
     ('Bank of Khartoum', 1913, 'CAT3', 'SDN'),
     ('Swazi Rail', 1963, 'CAT2', 'SWZ'),
     ('Tanzania Breweries Limited', 1933, 'CAT9', 'TZA'),
     ('La Poste du Togo', 1883, 'CAT16', 'TGO'),
     ('La Poste Tunisienne', 1847, 'CAT16', 'TUN'),
     ('Stanbic Bank Uganda Limited', 1906, 'CAT3', 'UGA'),
     ('ZamPost', 1896, 'CAT16', 'ZMB'),
     ('Standard Chartered Zimbabwe', 1892, 'CAT3', 'ZWE'),
     ('Spinzar Cotton Company', 1930, 'CAT1', 'AFG'),
     ('Azerbaijan Caspian Shipping Company', 1858, 'CAT2', 'AZE'),
     ('BMMI', 1883, 'CAT17', 'BHR'),
     ('M. M. Ispahani Limited', 1820, 'CAT11', 'BGD'),
     ('Tashi Group', 1959, 'CAT2', 'BTN'),
     ('Hua Ho Department Store', 1947, 'CAT17', 'BRN'),
     ('National Bank of Cambodia', 1954, 'CAT3', 'KHM'),
     ("Ma Yu Ching's Bucket Chicken House", 1153, 'CAT4', 'CHN'),
     ('Bank of Georgia', 1903, 'CAT3', 'GEO'),
     ('Wadia Group', 1736, 'CAT12', 'IND'),
     ('Pindad', 1808, 'CAT8', 'IDN'),
     ('North Oil Company', 1928, 'CAT10', 'IRQ'),
     ('Café Abu Salem', 1914, 'CAT4', 'ISR'),
     ('Kongō Gumi', 578, 'CAT6', 'JPN'),
     ('Arab Bank', 1930, 'CAT3', 'JOR'),
     ('Bogatyr Access Komir', 1913, 'CAT15', 'KAZ'),
     ('M.H. Alshaya Co.', 1890, 'CAT17', 'KWT'),
     ('Electricite du Laos', 1959, 'CAT10', 'LAO'),
     ('Bank Audi', 1830, 'CAT3', 'LBN'),
     ('Pos Malaysia', 1800, 'CAT16', 'MYS'),
     ('Mongolian National Broadcaster', 1931, 'CAT13', 'MNG'),
     ('Myanmar National Airlines', 1948, 'CAT2', 'MMR'),
     ('Nepal Bank Limited', 1937, 'CAT3', 'NPL'),
     ("Kim Chong-t'ae Electric Locomotive Works", 1945, 'CAT2', 'PRK'),
     ('Petroleum Development Oman', 1937, 'CAT10', 'OMN'),
     ('House of Habib', 1841, 'CAT5', 'PAK'),
     ('Destileria Limtuaco', 1853, 'CAT9', 'PHL'),
     ('Salam International Investment Limited', 1952, 'CAT5', 'QAT'),
     ('Petrodvorets Watch Factory', 1721, 'CAT7', 'RUS'),
     ('House of Alireza', 1845, 'CAT6', 'SAU'),
     ('Singapore Post', 1819, 'CAT16', 'SGP'),
     ('KT Corporation', 1885, 'CAT18', 'KOR'),
     ('George Steuart Group', 1835, 'CAT11', 'LKA'),
     ('Bakdash', 1885, 'CAT4', 'SYR'),
     ('Bank of Taiwan', 1897, 'CAT3', 'TWN'),
     ('B.Grimm Group', 1878, 'CAT5', 'THA'),
     ('Liwa Chemicals', 1939, 'CAT12', 'ARE'),
     ('Tashkent Aviation Production Association', 1932, 'CAT12', 'UZB'),
     ('Vietnam Railways', 1881, 'CAT2', 'VNM'),
     ('Yemenia Airways', 1962, 'CAT2', 'YEM'),
     ('ALBtelecom', 1912, 'CAT18', 'ALB'),
     ('Andbank', 1930, 'CAT3', 'AND'),
     ('Yerevan Ararat Brandy-Wine-Vodka Factory', 1877, 'CAT9', 'ARM'),
     ('St. Peter Stifts Kulinarium', 803, 'CAT4', 'AUT'),
     ('Olivaria Brewery', 1864, 'CAT9', 'BLR'),
     ('Affligem Brewery', 1074, 'CAT9', 'BEL'),
     ('Sarajevska Pivara', 1864, 'CAT9', 'BIH'),
     ('Arsenal AD', 1878, 'CAT8', 'BGR'),
     ('Kraljevica Shipyard', 1729, 'CAT12', 'HRV'),
     ('Bank of Cyprus', 1899, 'CAT3', 'CYP'),
     ('Pivovar Broumov', 1348, 'CAT9', 'CZE'),
     ('Munke Mølle', 1135, 'CAT12', 'DNK'),
     ('The Royal Mint', 886, 'CAT12', 'GBR'),
     ('Raeapteek', 1422, 'CAT14', 'EST'),
     ('Fiskars', 1649, 'CAT7', 'FIN'),
     ('Monnaie de Paris\xa0', 864, 'CAT12', 'FRA'),
     ('Staffelter\xa0Hof Winery', 862, 'CAT9', 'DEU'),
     ('Piraeus Bank', 1606, 'CAT3', 'GRC'),
     ('Zwack', 1790, 'CAT9', 'HUN'),
     ('Íslandspóstur', 1776, 'CAT16', 'ISL'),
     ("Sean's Bar", 900, 'CAT4', 'IRL'),
     ('Marinelli Bell Foundry', 1040, 'CAT12', 'ITA'),
     ('Meridian Corporation', 1999, 'CAT13', 'XK'),
     ('Cēsu Alus', 1590, 'CAT9', 'LVA'),
     ('National Bank of Liechtenstein', 1861, 'CAT3', 'LIE'),
     ('Gubernija', 1665, 'CAT9', 'LTU'),
     ('Mousel', 1511, 'CAT9', 'LUX'),
     ('HSBC Bank Malta', 1882, 'CAT3', 'MLT'),
     ('Pošta Crne Gore', 1841, 'CAT16', 'MNE'),
     ('The Brand Brewery', 1340, 'CAT9', 'NLD'),
     ('Tutunski kombinat Prilep', 1873, 'CAT7', 'MKD'),
     ('Posten Norge', 1647, 'CAT16', 'NOR'),
     ('Bochnia Salt Mine', 1248, 'CAT15', 'POL'),
     ('CTT-Correios de Portugal SA', 1520, 'CAT16', 'PRT'),
     ('Ursus Breweries', 1878, 'CAT9', 'ROU'),
     ('Apatin Brewery', 1756, 'CAT9', 'SRB'),
     ('Kremnica Mint', 1328, 'CAT12', 'SVK'),
     ('Gostilna Gastuž', 1467, 'CAT19', 'SVN'),
     ('Casa de Ganaderos', 1218, 'CAT1', 'ESP'),
     ('Skyllbergs bruk', 1346, 'CAT12', 'SWE'),
     ('Gasthof Sternen', 1230, 'CAT19', 'CHE'),
     ('Çemberlitaş Hamamı', 1584, 'CAT19', 'TUR'),
     ('Drohobych salt plant', 1250, 'CAT12', 'UKR'),
     ('Mount Gay Rum', 1703, 'CAT9', 'BRB'),
     ('Belize Bank', 1902, 'CAT3', 'BLZ'),
     ("Hudson's Bay Company", 1670, 'CAT17', 'CAN'),
     ('Florida Ice and Farm Company', 1908, 'CAT9', 'CRI'),
     ('Cubana de Aviación', 1929, 'CAT2', 'CUB'),
     ('The Chronicle (Dominica)', 1909, 'CAT13', 'DMA'),
     ('HSBC El Salvador', 1891, 'CAT3', 'SLV'),
     ('Corporación Multi Inversiones', 1920, 'CAT11', 'GTM'),
     ('Rhum Barbancourt', 1862, 'CAT9', 'HTI'),
     ('National Railroad of Honduras', 1870, 'CAT2', 'HND'),
     ('Rose Hall', 1770, 'CAT19', 'JAM'),
     ('La Casa de Moneda de México', 1534, 'CAT12', 'MEX'),
     ('Flor de Caña', 1890, 'CAT9', 'NIC'),
     ('National Bank of Panama', 1904, 'CAT3', 'PAN'),
     ('1st National Bank of St Lucia', 1938, 'CAT3', 'LCA'),
     ('House of Angostura', 1830, 'CAT9', 'TTO'),
     ('Shirley Plantation', 1638, 'CAT1', 'USA'),
     ('Bank of the Province of Buenos Aires', 1822, 'CAT3', 'ARG'),
     ('Banco Nacional de Bolivia', 1871, 'CAT3', 'BOL'),
     ('Casa da Moeda do Brasil', 1694, 'CAT12', 'BRA'),
     ('Famae', 1811, 'CAT8', 'CHL'),
     ('Casa de Moneda de Colombia', 1621, 'CAT12', 'COL'),
     ('Banks DIH', 1840, 'CAT11', 'GUY'),
     ('Casa Nacional de Moneda', 1565, 'CAT3', 'PER'),
     ('Cafe Brasilero', 1877, 'CAT4', 'URY'),
     ('Hacienda Chuao', 1660, 'CAT11', 'VEN'),
     ('Australia Post', 1809, 'CAT16', 'AUS'),
     ('Bank of New Zealand', 1861, 'CAT3', 'NZL'),
     ('European Trust Company', 1991, 'CAT3', 'VUT')]




```python

```


```python

```

pd.read_sql('''SELECT * FROM users u LEFT JOIN orders o ON u.user_id = o.user_id''', conn)


```python

```
