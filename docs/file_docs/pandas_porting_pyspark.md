# Own docs regarding porting over from Pandas to Pyspark.

I pandas infererade(beräknade schema automatiskt) jag schema automatiskt och med PySpark definierar jag det EXPLICIT.
Det är med flit och ett designbeslut. Jag vill inte att PySpark ska gissa vad mina silver columns ska heta eller ha för data typ. Det är mitt kontrakt mot golden layer och det ska vara explicit och hållas stabilt.

Med/I Pandas anropade jag _flatten() med iterrows(), rad för rad i Python. I PySpark använder man en UDF (User Defined Function) som Spark anropar parallellt på varje rad i sina distribuerade partitioner. En UDF är en bro mellan Python kod och Sparks exekveringsmotor.

**Notera:** UDFs är långsammare än inbyggda Spark-funktioner eftersom data måste serialiseras mellan JVM och Python. Men för komplex nested JSON-logik är de det enklaste och tydligaste alternativet enligt den teori jag läst.


### Det viktigaste att förstå rent konceptuellt:

Den viktigaste konceptuella skillnaden att ta med sig härifrån är hur flattenning hanteras nu med PySpark. I Pandas versionen itererade jag rad för rad med iterrows(), en Python for loop som körs sekventiellt. Nu i PySpark versionen registreras en UDF som Spark anropar parallellt på alla rader samtidigt, fördelat över dina(datorns) CPU kärnor. Det ska vara här som PySpark börjar visa sitt värde när datamängderna växer och Pandas ger dig OOM issues.

**Viktigt att veta OM jag undrar VARFÖR det går mycket långsammare än Pandas**
- Spark har en overhead när det kommer till uppstart, den saknar Pandas. MEN när min datamängd växer(Endast månad 03 och några dagar in på månad 04 vid skrivande tillfälle) så kommer jag märka skillnaden och se styrkan med PySpark Vs Pandas.
