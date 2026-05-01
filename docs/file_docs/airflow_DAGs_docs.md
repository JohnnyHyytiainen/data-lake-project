# Own docs for airflow and Directed Acyclic Graphs (DAGs) and its importance.


## Simplified explanations on DAGs

De DAG diagram som visas vid sökning ser mer invecklade ut än vad det egentligen är. DAG akronymen står för **Directed Acyclic Graph** vilket i stora drag innebär ett mönster som jag redan är van vid att både bygga, tänka kring och jobba med. Enkelt förklarat med nuvarande konceptuell förståelse är detta:

- Med min nuvarande pipeline och de pipes jag tidigare byggt så flödar datan genom BRONZE -> SILVER -> GOLD. Varje pil pekar framåt, *ALDRIG* bakåt. Jag likadant som att datan alltid ska flöda framåt och inte t.ex från Silver till Bronze, dbt kan aldrig köras innan bronze har flyttat klart datan från bronze till silver. **DET** är en *DAG*.

- Det **DIRECTED** betyder är att att pilarna *HAR* en riktning.  
- Det **ACYCLIC** betyder är att det *ALDRIG* finns en cirkel. Ett steg kan aldrig trigga sig självt direkt eller indirekt.  
- Det **GRAPH** betyder är enbart det matematiska ordet för ett diagram, med nodes och kanter. 

    - **DAGs** är alltså ett flödesschema där pilarna *alltid* pekar framåt och *ALDRIG* bildar loopar. Det är "basically" allting. Rent konceptuellt dvs, i verkligheten är det en helt annan sak, att bygga det och förstå det på djupet är ett annat monster, det är därför jag ger mig på ett försök till att lyckas få det att fungera.

- I stora drag så ser det ungefär ut så här: 'Gör steg A, sen steg B och C parallelt, sen gör steg D när *BÅDE* steg B och C är klara.

## 