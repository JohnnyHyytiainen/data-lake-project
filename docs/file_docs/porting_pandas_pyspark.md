# Own docs regarding porting over from Pandas to Pyspark.

## TODO: Update docs, re-write it clean, its kind of double now.

I pandas infererade(beräknade schema automatiskt) jag schema automatiskt och med PySpark definierar jag det EXPLICIT.
Det är med flit och ett designbeslut. Jag vill inte att PySpark ska gissa vad mina silver columns ska heta eller ha för data typ. Det är mitt kontrakt mot golden layer och det ska vara explicit och hållas stabilt.

Med/I Pandas anropade jag _flatten() med iterrows(), rad för rad i Python. I PySpark använder man en UDF (User Defined Function) som Spark anropar parallellt på varje rad i sina distribuerade partitioner. En UDF är en bro mellan Python kod och Sparks exekveringsmotor.

**Notera:** UDFs är långsammare än inbyggda Spark-funktioner eftersom data måste serialiseras mellan JVM och Python. Men för komplex nested JSON-logik är de det enklaste och tydligaste alternativet enligt den teori jag läst.


### Det viktigaste att förstå rent konceptuellt:

Den viktigaste konceptuella skillnaden att ta med sig härifrån är hur flattenning hanteras nu med PySpark. I Pandas versionen itererade jag rad för rad med iterrows(), en Python for loop som körs sekventiellt. Nu i PySpark versionen registreras en UDF som Spark anropar parallellt på alla rader samtidigt, fördelat över dina(datorns) CPU kärnor. Det ska vara här som PySpark börjar visa sitt värde när datamängderna växer och Pandas ger dig OOM issues.

**Viktigt att veta OM jag undrar VARFÖR det går mycket långsammare än Pandas**
- Spark har en overhead när det kommer till uppstart, den saknar Pandas. MEN när min datamängd växer(Endast månad 03 och några dagar in på månad 04 vid skrivande tillfälle) så kommer jag märka skillnaden och se styrkan med PySpark Vs Pandas.


# Own notes written in different document thats removed. 

Pandas läser all data in i minnet på en gång och jobbar med den som en enda tabell. Det fungerar utmärkt för 58 000 records, men om en hade haft t.ex 58 miljoner records skulle ens dator gå på sina knän. PySpark tänker istället i termer av distribuerade operationer, den delar upp data i partitioner och bearbetar dem parallellt, potentiellt på hundratals maskiner. 

Just nu lokalt på min dator kör den fortfarande parallellt fast på mina CPU-cores istället. Det är därför `local[*]` i min SparkSession betyder "använd alla tillgängliga kärnor"

Den praktiska konsekvensen för mig är att PySpark aldrig läser hela datasetet in i minnet på en gång. När jag skriver en transformation beskriver du vad som ska hända, inte hur det ska exekveras. Spark bygger upp en exekveringsplan och kör allting när jag faktiskt behöver resultatet, det är detta som kallas för `lazy evaluation`. Det är som att skriva ett recept kontra att faktiskt laga maten. Pandas lagar maten direkt(`Eager evaluation`). Spark skriver receptet och lagar det sedan optimalt när tallriken behövs.

Den viktigaste konceptuella skillnaden att ta med sig härifrån är hur flattenning hanteras. I min Pandas version itererade jag rad för rad med `iterrows()` en Python `for loop` som körs sekventiellt. I PySpark versionen registrerar jag en UDF som Spark anropar parallellt på alla rader samtidigt, fördelat över mina CPU-cores. Det är egentligen här som PySpark börjar visa sitt värde när datamängderna växer.

En UDF i PySpark innebär att data måste serialiseras från JVM (där Spark lever) till Python för att sedan bearbetas i Python, och sedan serialiseras tillbaka till JVM. Det är som att skicka ett paket från Sverige till USA för att öppna det och sedan skicka tillbaka innehållet, onödigt dyrt och onödigt tidskrävande. `get_json_object` däremot körs direkt i JVM utan att lämna Spark motorn. Det är snabbare, mer minneseffektivt, och skalbart på ett sätt som UDFer inte är.

`zero padding` problemet kringgick jag med `date_format("MM")` och `date_format("dd")` det är en liten men kritisk detalj för att min Hive-partitionering ska fungera konsekvent. month=3 och month=03 är två olika mappar för ett filsystem men ska vara samma partition logiskt sett.


----
**TODO:** 
- Lös på mer om zero padding och get_json_object ännu mer.