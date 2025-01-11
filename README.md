# **ETL proces datasetu IMDb**

Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre analýzu dát z IMDb datasetu. Projekt sa zameriava na spracovanie a transformáciu údajov o filmoch, hercoch a hodnoteniach. 
Výsledný dátový model umožňuje efektívnu analýzu a vizualizáciu kľúčových metrík, ako sú priemerné hodnotenia, žánrové trendy alebo popularita hercov.

---

## **1. Úvod a popis zdrojových dát**

Cieľom projektu je analyzovať údaje týkajúce sa filmov, hercov a ich hodnotení. Táto analýza umožňuje lepšie pochopiť preferencie divákov, trendy vo filmovom priemysle a výkony hercov.

Zdrojové dáta pochádzajú z Kaggle datasetu, ktorý obsahuje nasledujúce hlavné tabuľky:

- `movies`: Informácie o tituloch, žánroch a roku vydania.
- `actors`: Informácie o hercoch, ktorí účinkovali vo filmoch.
- `ratings`: Údaje o hodnoteniach jednotlivých filmov.

---

###  **1.1. Architektúra dátového modelu**

### **ERD diagram**

Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na entitno-relačnom diagrame (ERD).

<p align="center"> <img src="https://github.com/KrisztianVendegh/databazove/blob/main/Imdb_db/ER%20Diagram.png" alt="ERD Schema"> <br> <em>Obrázok 1: Entitno-relačná schéma IMDb</em> </p>

---

## **2.Dimenzionálny model**

Navrhnutý bol hviezdicový model (star schema), kde centrálny bod predstavuje faktová tabuľka fact_ratings,
ktorá je prepojená s nasledujúcimi dimenziami:

- **`dim_movies`**: Obsahuje informácie o tituloch a žánroch filmov.
- **`dim_actors`**: Obsahuje informácie o hercoch, ktorí sa podieľali na filmoch.
  
- **`dim_date`**: Informácie o dátumoch hodnotení (deň, mesiac, rok).

<p align="center"> <img src="https://github.com/KrisztianVendegh/databazove/blob/main/Imdb_db/Star%20schema.png" ><br> <em>Obrázok 2 Schéma hviezdy pre AmazonBooks</em> </p>

---

## **3. ETL proces v Snowflake**

ETL proces pozostával z troch hlavných fáz: extrahovanie (Extract), transformácia (Transform) a načítanie (Load).
Tento proces bol implementovaný v Snowflake s cieľom pripraviť zdrojové dáta 
zo staging vrstvy do viacdimenzionálneho modelu vhodného na analýzu a vizualizáciu.

---

### **3.1 Extract (Extrahovanie dát)**

Dáta zo zdrojového datasetu (formát .csv) boli najprv nahraté do Snowflake prostredníctvom interného stage úložiska s názvom my_stage.
Stage v Snowflake slúži ako dočasné úložisko na import alebo export dát. Vytvorenie stage bolo zabezpečené príkazom:

Príklad kódu:
```sql
CREATE OR REPLACE STAGE GIRAFFE_IMDb_stage;
```
Do stage boli následne nahraté súbory obsahujúce údaje o filmoch, žánroch, krajinách a jazykoch. 
Dáta boli importované do staging tabuliek pomocou príkazu COPY INTO. Pre každú tabuľku sa použil podobný príkaz:

```sql
COPY INTO imdb_dataset 
FROM @GIRAFFE_IMDb_STAGE/IMDb_Dataset.csv 
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
```
V prípade nekonzistentných záznamov bol použitý parameter ON_ERROR = 'CONTINUE', 
ktorý zabezpečil pokračovanie procesu bez prerušenia pri chybách.

---

### **3.2 Transform (Transformácia dát)**

V tejto fáze boli dáta zo staging tabuliek vyčistené, transformované a obohatené. 
Hlavným cieľom bolo pripraviť dimenzie a faktovú tabuľku, ktoré umožnia jednoduchú a efektívnu analýzu.

Dimenzie boli navrhnuté na poskytovanie kontextu pre faktovú tabuľku. Genre_Dim obsahuje údaje o žánroch filmov vrátane primárneho a sekundárneho žánru. 
Transformácia zahŕňala priradenie správnych hodnotení filmov k príslušným žánrom.

```sql
CREATE TABLE Genre_Dim AS
SELECT DISTINCT
    g.Genre_ID,
    g.Genre,
    g.Second_Genre
FROM Genre_Dim g;
```

Dimenzia Country_Dim obsahuje údaje o krajinách, v ktorých boli filmy produkované, a Language_Dim uchováva informácie o jazykoch použitých v filmoch.
Tieto dimenzie sú vytvorené pomocou jednoduchého výberu z staging tabuliek.

```sql
CREATE TABLE Country_Dim AS
SELECT DISTINCT
    c.Country_ID,
    c.Country
FROM Country_Dim c;

CREATE TABLE Language_Dim AS
SELECT DISTINCT
    l.Language_ID,
    l.Language
FROM Language_Dim l;
```
Faktová tabuľka Film_Fact obsahuje záznamy o filmoch a prepojenia na všetky dimenzie. 
Táto tabuľka obsahuje kľúčové metriky, ako je rozpočet, tržby z kin, hodnotenie IMDb a dĺžka filmu.

```sql
CREATE TABLE Film_Fact AS
SELECT 
    f.Film_ID,
    f.Budget_Million,
    f.Box_Office_Million,
    f.IMDb_Rating,
    f.MetaScore,
    f.Duration_Minutes,
    g.Genre_ID,
    c.Country_ID,
    l.Language_ID
FROM Film_Fact f
JOIN Genre_Dim g ON f.Genre_ID = g.Genre_ID
JOIN Country_Dim c ON f.Country_ID = c.Country_ID
JOIN Language_Dim l ON f.Language_ID = l.Language_ID;
```

### **3.3 Load (Načítanie dát)**

Po úspešnom vytvorení dimenzií a faktovej tabuľky boli dáta nahraté do finálnej štruktúry.
Na záver boli staging tabuľky odstránené, aby sa optimalizovalo využitie úložiska:

```sql
DROP TABLE IF EXISTS Genre_Dim;
DROP TABLE IF EXISTS Country_Dim;
DROP TABLE IF EXISTS Language_Dim;
DROP TABLE IF EXISTS Film_Fact;
```
ETL proces v Snowflake umožnil spracovanie pôvodných dát z .csv formátu do viacdimenzionálneho modelu typu hviezda. Tento proces zahŕňal čistenie, 
obohacovanie a reorganizáciu údajov. Výsledný model umožňuje analýzu filmov a poskytuje základ pre vizualizácie a reporty.

---

## **4 Vizualizácia dát**

Dashboard obsahuje 6 vizualizácií, ktoré poskytujú základný prehľad o kľúčových metrikách a trendoch týkajúcich sa filmov, žánrov, krajín a jazykov.
Tieto vizualizácie odpovedajú na dôležité otázky a umožňujú lepšie pochopiť správanie používateľov a ich preferencie.

<p align="center"> <img src="https://github.com/KrisztianVendegh/databazove/blob/main/Imdb_db/graph.JPG" alt="ERD Schema"> <br> <em>Obrázok 3 Dashboard Movie datasetu</em> </p>

### **Graf 1: Najviac hodnotené filmy (Top 10 filmov)**

Táto vizualizácia zobrazuje 10 filmov s najväčším priemerným hodnotením. 
Umožňuje identifikovať najlepšie hodnotené tituly. Zistíme napríklad, že film Inception má najvyššie hodnotenie medzi filmami. 
Tieto informácie môžu byť užitočné na odporúčanie filmov alebo marketingové kampane.

```sql
SELECT 
    Title,
    AVG(IMDb_rating) AS Average_Rating
FROM imdb_dataset
GROUP BY Title
ORDER BY Average_Rating DESC
LIMIT 10;
```
---

### **Graf 2: Najdlhšie filmy (Top 10 najdlhších filmov)**

Graf zobrazuje 10 filmov s najdlhšou dĺžkou trvania. Z údajov môžeme zistiť, že film The Godfather patrí medzi najdlhšie filmy.
Tieto informácie sú užitočné pri plánovaní sledovania filmov podľa času.

```sql
SELECT 
    DISTINCT Title,
    Duration_minutes
FROM imdb_dataset
ORDER BY Duration_minutes DESC
LIMIT 10;
```
---
### **Graf 3: Počet filmov podľa krajín**

Tento graf ukazuje počet filmov podľa krajín. Z údajov môžeme zistiť, 
že najviac filmov pochádza z USA, čo môže byť užitočné na analýzu filmového priemyslu v rôznych regiónoch.

```sql
SELECT 
    Country,
    COUNT(*) AS Film_Count
FROM imdb_dataset_5
GROUP BY Country
ORDER BY Film_Count DESC;
```
---

### **Graf 4: Leonardo DiCaprio filmov podľa hodnotenia**

Tento graf zobrazuje filmy, v ktorých Leonardo DiCaprio hral, a ich hodnotenia. Zistíme napríklad, že film Titanic má vysoké hodnotenie. 
Tieto informácie môžu byť užitočné pri výbere filmov s obľúbenými hercami.

```sql
SELECT 
    DISTINCT Title,
    IMDb_rating
FROM imdb_dataset
WHERE Star_Cast LIKE '%Leonardo DiCaprio%'
ORDER BY IMDb_rating DESC;
```
---
### **Graf 5: Filmy podľa žánrov (Fantasy)**

Graf zobrazuje filmy, ktoré patria do žánru Fantasy. Tento graf umožňuje identifikovať najlepšie hodnotené fantasy filmy a získať prehľad o najpopulárnejších tituloch v tomto žánri.

```sql
SELECT 
    Title,
    Genre
FROM imdb_dataset
WHERE Genre LIKE '%Fantasy%'
ORDER BY Title;
```
Dashboard poskytuje komplexný pohľad na dáta, pričom zodpovedá dôležité otázky týkajúce sa filmov, hodnotení, krajín, žánrov a hercov. 
Vizualizácie umožňujú jednoduchú interpretáciu dát a môžu byť využité na optimalizáciu odporúčacích systémov, marketingových stratégií a filmových služieb.

*Autor:* Krisztián Vendégh
