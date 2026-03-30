# Own docs for bootstrap_historical.py script

Min förståelse av vad `bootstrap_historical.py` är för något och varför det scriptet spelar roll.

Fram tills nu har jag "fiskat i ett extremt stort hav med en **väldigt** liten håv". Ett event här och ett event där beroende på vad Github råkar producera just när min producer vaknar till liv var 120sek. Det har i skrivandes stund gett mig 82 silver records på två kvällar(Söndag 29/03 och måndag 30/03). 

Det är tillräckligt av ett Proof of Concept för att visa att pipen fungerar med producer-kafka-consumer och Githubs API. **MEN** det är långt ifrån tillräckligt för att mitt val av `PySpark` ska vara meningsfullt att använda, eller för att gold layers aggregeringar ska ge något typ av intressanta svar.

**Lösningen på det?**
- Lösningen heter 'Github Archive'
    - Sen 2011 har någon(galning?) spelat in varje publikt Github-event och lagrat dom som komprimerade JSON filer, en fil per timme... Den datan finns tillgänglig på `https://data.gharchive.org/`

En timmes fil ifrån ett aktivt år *kan* innehålla hundratusentals events. Det är härifrån jag kommer kunna rättfärdiga min användning av deps så som PySpark. Jag behöver stora mängder med data för att det ens ska vara värt det.