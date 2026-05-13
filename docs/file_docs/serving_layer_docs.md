# Own docs for serving layer, and its purpose.

Egna docs kring serving layer.
---
Serving layer eller visningslagret/serveringslager är oftast den sista delen i data-arkitekturen(OFTAST inom lambda-arkitektur(Läs på mer om denna) och `Medallion`-arkitektur/`gold`-layer). Syftet med serving layer är att leverera bearbetad(aggregerad/berikad), strukturerad och affärs ready data till stakeholders/end users, BI-verktyg(Business intelligence) eller ML modeller.

**Serving-lagrets roll i medallion arkitektur**  

*VARFÖR hör Grafana hemma här? Jag har tre lager:*  

* Bronze lagrar raw data.

* Silver transformerar och validerar den datan.

* Gold aggregerar och svarar på business frågor(Eller de frågor en nu vill ta reda på).

    * Men Gold är fortfarande bara filer på disk(lokalt) eller sparade på annan plats(cloud storage etc). Det finns inget värde för en människa med tusentals/miljontals med filer som är svårlästa eller väggar av text, det säger inte så mycket. Värdet i datan existerar inte förens en människa kan **SE** svaren ifrån all data.

* Serving lagret är bryggan mellan Golds aggregerade data och de riktiga insikterna
    * Exempel: En pipeline utan serving är som en bok på ett språk ingen kan läsa: all information finns där, men ingen kan läsa boken eller ens förstå den.

--- 
**Vad är serving lagret?**  

- Det här lagret är oftast optimerat för läsning. Dvs optimerat för snabba queries, pre-aggregated(Pre joined etc. Dvs, det jag gjorde i DBT med datan)  

- Det minskar belastningen och ska ej sköta tunga lyft, det sker i tidigare lager. Se punkt ovan, OPTIMERAT för hastighet.  

- 

-

---
